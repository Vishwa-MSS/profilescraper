import time
import re
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple
import zipfile

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from urllib.parse import urljoin


# -----------------------------
# CONFIG
# -----------------------------
# Men's formats
FORMATS_MENS = {
    "Test": 1,
    "ODI": 2,
    "T20I": 3,
    "First-class": 4,
    "List A": 5,
    "T20": 6,
}

# Women's formats
FORMATS_WOMENS = {
    "WTest": 8,
    "WODI": 9,
    "WT20I": 10,
    "Women's First-class": 4,
    "Women's List A": 5,
    "Women's T20": 6,
}

VIEWS = {
    "Batting_Innings": "type=batting;view=innings",
    "Bowling_Innings": "type=bowling;view=innings",
    "Fielding_Innings": "type=fielding;view=innings",
    "Batting_Summary": "type=batting",
    "Bowling_Summary": "type=bowling",
    "Fielding_Summary": "type=fielding",
}

BASE_URL = (
    "https://stats.espncricinfo.com/ci/engine/player/{player_id}.html"
    "?class={class_id};template=results;{params}"
)

PROFILE_URL = "https://www.espncricinfo.com/ci/content/player/{player_id}.html"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    )
}


# -----------------------------
# SCRAPING FUNCTIONS
# -----------------------------
def get_all_pages(start_url, session, max_pages=50, delay=0.6):
    pages = []
    visited = set()
    url = start_url

    for _ in range(max_pages):
        if not url or url in visited:
            break

        visited.add(url)
        resp = session.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()

        html = resp.text
        pages.append((url, html))

        soup = BeautifulSoup(html, "html.parser")
        next_link = None

        for a in soup.find_all("a", href=True):
            text = a.get_text(" ", strip=True).lower()
            href = a["href"]
            if "next" in text:
                next_link = urljoin(url, href)
                break

        url = next_link
        time.sleep(delay)

    return pages


def pick_best_table(tables, view_name):
    if not tables:
        return None

    batting_keywords = {"runs", "bf", "4s", "6s", "sr", "mins"}
    bowling_keywords = {"overs", "mdns", "runs", "wkts", "econ", "avg", "sr"}

    best = None
    best_score = -1

    for df in tables:
        cols = {str(c).strip().lower() for c in df.columns}
        if "batting" in view_name.lower():
            score = len(cols & batting_keywords)
        elif "bowling" in view_name.lower():
            score = len(cols & bowling_keywords)
        else:
            score = len(cols)

        score += df.shape[0] * 0.01

        if score > best_score:
            best_score = score
            best = df

    return best if best is not None else tables[0]


def clean_dataframe(df):
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(axis=0, how="all")
    df = df.dropna(axis=1, how="all")
    return df.reset_index(drop=True)


def fetch_table_for_url(url, session, view_name):
    pages = get_all_pages(url, session)
    all_parts = []

    for page_url, html in pages:
        try:
            tables = pd.read_html(html)
        except ValueError:
            continue

        df = pick_best_table(tables, view_name)
        if df is None:
            continue

        df = clean_dataframe(df)
        df["source_url"] = page_url
        all_parts.append(df)

    if not all_parts:
        return pd.DataFrame()

    combined = pd.concat(all_parts, ignore_index=True)
    combined = combined.drop_duplicates().reset_index(drop=True)
    return combined


def safe_sheet_name(name: str) -> str:
    for ch in ['\\', '/', '*', '[', ']', ':', '?']:
        name = name.replace(ch, "_")
    return name[:31]


def safe_filename(name: str) -> str:
    """Make a safe filename by removing/replacing invalid characters."""
    for ch in ['\\', '/', '*', '[', ']', ':', '?', '<', '>', '|', '"']:
        name = name.replace(ch, "_")
    return name.strip()


# -----------------------------
# PLAYER PROFILE SCRAPE (fallback)
# -----------------------------
def scrape_player_profile(player_id: int, session: requests.Session) -> dict:
    url = PROFILE_URL.format(player_id=player_id)
    resp = session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    name = ""
    h1 = soup.find("h1")
    if h1:
        name = h1.get_text(" ", strip=True)

    info_map = {}
    for div in soup.find_all("div", class_=re.compile(r"ciPlayerinformationtxt")):
        b = div.find("b")
        if not b:
            continue
        label = b.get_text(" ", strip=True).strip(":").lower()
        value = div.get_text(" ", strip=True)
        value = re.sub(
            r"^\s*" + re.escape(b.get_text(" ", strip=True)) + r"\s*",
            "",
            value
        ).strip()
        info_map[label] = value

    return {
        "Player Name": name,
        "Full Name": info_map.get("full name", ""),
        "Born": info_map.get("born", ""),
        "Age": info_map.get("current age", info_map.get("age", "")),
        "Batting Style": info_map.get("batting style", ""),
        "Bowling Style": info_map.get("bowling style", ""),
        "Playing Role": info_map.get("playing role", ""),
        "Height": info_map.get("height", ""),
        "Education": info_map.get("education", ""),
        "Nicknames": info_map.get("nicknames", ""),
        "Fielding Position": info_map.get("fielding position", ""),
        "Also Known As": info_map.get("also known as", ""),
        "Other": info_map.get("other", ""),
        "Died": info_map.get("died", ""),
    }


# -----------------------------
# INPUT FILE HELPERS
# -----------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def read_players_file(uploaded_file) -> pd.DataFrame:
    """Reads CSV/XLSX/XLS correctly and normalizes player_id to int."""
    fname = uploaded_file.name.lower()

    if fname.endswith(".csv"):
        df = pd.read_csv(uploaded_file, encoding="utf-8-sig")
    else:
        df = pd.read_excel(uploaded_file)

    df = normalize_columns(df)

    required = {"player_id", "Country", "Player Name"}
    if not required.issubset(df.columns):
        raise ValueError(f"Missing required columns. Need {required}, found {set(df.columns)}")

    # Clean text
    df["Country"] = df["Country"].astype(str).str.strip()
    df["Player Name"] = df["Player Name"].astype(str).str.strip()

    # CRITICAL: normalize player_id (fixes 253802.0 vs 253802 problems)
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce")
    df = df.dropna(subset=["player_id"]).copy()
    df["player_id"] = df["player_id"].astype(int)
    
    # Add Gender column if not present
    if "Gender" not in df.columns:
        df["Gender"] = "Men"  # Default to Men if not specified

    return df


def build_player_info_row_from_file(df_players: pd.DataFrame, player_id: int) -> dict:
    if df_players is None or df_players.empty or "player_id" not in df_players.columns:
        return {}

    pid = int(player_id)
    row = df_players.loc[df_players["player_id"] == pid]
    if row.empty:
        return {}

    row = row.iloc[0].to_dict()

    return {
        "Player Name": row.get("Player Name", ""),
        "Country": row.get("Country", ""),
        "Gender": row.get("Gender", "Men"),
        "Full Name": row.get("Full Name", ""),
        "Born": row.get("Born", ""),
        "Age": row.get("Age", ""),
        "Batting Style": row.get("Batting Style", ""),
        "Bowling Style": row.get("Bowling Style", ""),
        "Playing Role": row.get("Playing Role", ""),
        "Height": row.get("Height", ""),
        "Education": row.get("Education", ""),
        "Nicknames": row.get("Nicknames", ""),
        "Fielding Position": row.get("Fielding Position", ""),
        "Also Known As": row.get("Also Known As", ""),
        "Other": row.get("Other", ""),
        "Died": row.get("Died", ""),
    }


def detect_gender_from_formats(player_id: int, session: requests.Session) -> str:
    """
    Detect player gender by checking which formats have data.
    Women's formats: WTest (8), WODI (9), WT20I (10)
    Men's formats: Test (1), ODI (2), T20I (3)
    """
    # Check women's formats first
    for format_name, class_id in [("WODI", 9), ("WT20I", 10), ("WTest", 8)]:
        url = BASE_URL.format(
            player_id=player_id,
            class_id=class_id,
            params="type=batting"
        )
        try:
            resp = session.get(url, headers=HEADERS, timeout=10)
            if resp.status_code == 200:
                tables = pd.read_html(resp.text)
                if tables and len(tables) > 0 and len(tables[0]) > 0:
                    return "Women"
        except:
            continue
    
    return "Men"


# -----------------------------
# MAIN GENERATION
# -----------------------------
def generate_excel(player_id: int, match_id: str | None, df_players: pd.DataFrame | None, 
                   gender: str = "Men", progress_callback=None) -> Tuple[bytes, str]:
    """
    Generate Excel file for a single player.
    Returns: (excel_bytes, player_name)
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    base_info = build_player_info_row_from_file(df_players, player_id) if df_players is not None else {}

    # Only scrape if file didn't provide useful info
    scraped_info = {}
    if not base_info or all((pd.isna(v) or str(v).strip() == "") for v in base_info.values()):
        try:
            scraped_info = scrape_player_profile(player_id, session)
        except Exception:
            scraped_info = {}

    # Merge: scraped first, file second (file overrides)
    player_info = {}
    player_info.update(scraped_info)
    player_info.update({k: v for k, v in (base_info or {}).items() if (not pd.isna(v) and str(v).strip() != "")})

    player_info.setdefault("Player Name", "")
    player_info.setdefault("Country", "")
    player_info.setdefault("Gender", gender)

    player_name = player_info.get("Player Name", f"Player_{player_id}")

    # Select formats based on gender
    if gender.lower() == "women":
        FORMATS = FORMATS_WOMENS
    else:
        FORMATS = FORMATS_MENS

    player_info_sheet_cols = [
        "Player Name", "Country", "Gender", "Match ID",
        "Full Name", "Born", "Age",
        "Batting Style", "Bowling Style", "Playing Role",
        "Height", "Education", "Nicknames",
        "Fielding Position", "Also Known As", "Other", "Died"
    ]

    player_info_row = {c: "" for c in player_info_sheet_cols}
    player_info_row.update(player_info)
    player_info_row["Match ID"] = match_id or ""
    player_info_df = pd.DataFrame([player_info_row], columns=player_info_sheet_cols)

    index_rows = []
    all_data_parts = []

    out_xlsx = BytesIO()
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        player_info_df.to_excel(writer, sheet_name="PLAYER_INFO", index=False)

        total_views = len(FORMATS) * len(VIEWS)
        current_view = 0

        for format_name, class_id in FORMATS.items():
            for view_name, params in VIEWS.items():
                current_view += 1
                if progress_callback:
                    progress_callback(current_view, total_views)
                
                url = BASE_URL.format(player_id=player_id, class_id=class_id, params=params)
                try:
                    df = fetch_table_for_url(url, session, view_name)

                    if df.empty:
                        index_rows.append({"Format": format_name, "View": view_name, "Rows": 0, "URL": url, "Sheet": ""})
                        continue

                    df.insert(0, "Player ID", player_id)
                    df.insert(1, "Format", format_name)
                    df.insert(2, "View", view_name)

                    sheet_name = safe_sheet_name(f"{format_name}_{view_name}")
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    all_data_parts.append(df)

                    index_rows.append({"Format": format_name, "View": view_name, "Rows": len(df), "URL": url, "Sheet": sheet_name})

                except Exception as e:
                    index_rows.append({"Format": format_name, "View": view_name, "Rows": 0, "URL": url, "Sheet": "", "Error": str(e)})

        if all_data_parts:
            pd.concat(all_data_parts, ignore_index=True).to_excel(writer, sheet_name="ALL_DATA", index=False)

        pd.DataFrame(index_rows).to_excel(writer, sheet_name="INDEX", index=False)

    return out_xlsx.getvalue(), player_name


def process_single_player(player_id: int, player_name: str, match_id: str, 
                         df_players: pd.DataFrame, gender: str = "Men") -> Dict:
    """Process a single player and return result dictionary."""
    try:
        excel_data, retrieved_name = generate_excel(player_id, match_id, df_players, gender)
        return {
            "status": "success",
            "player_id": player_id,
            "player_name": retrieved_name or player_name,
            "gender": gender,
            "data": excel_data,
            "error": None
        }
    except Exception as e:
        return {
            "status": "error",
            "player_id": player_id,
            "player_name": player_name,
            "gender": gender,
            "data": None,
            "error": str(e)
        }


def create_zip_file(results: List[Dict]) -> bytes:
    """Create a zip file containing all successful Excel files."""
    zip_buffer = BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for result in results:
            if result["status"] == "success" and result["data"]:
                # Create safe filename
                filename = f"{result['player_id']}_{safe_filename(result['player_name'])}.xlsx"
                # Add file to zip
                zip_file.writestr(filename, result["data"])
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()


# -----------------------------
# STREAMLIT UI
# -----------------------------
st.set_page_config(page_title="Cricinfo Multi-Player Stats Downloader", layout="wide")
st.title("🏏 Cricinfo Multi-Player Stats Downloader")

# Initialize session state
if "results" not in st.session_state:
    st.session_state.results = []
if "processing" not in st.session_state:
    st.session_state.processing = False

uploaded = st.file_uploader(
    "Upload your Players file (must include: Player Name, Country, player_id)",
    type=["xlsx", "xls", "csv"]
)

df_players = None
if uploaded:
    try:
        df_players = read_players_file(uploaded)
        st.success(f"✅ Loaded {len(df_players)} players from file")
    except Exception as e:
        st.error(f"❌ Error reading file: {str(e)}")
        st.stop()

col1, col2 = st.columns([1, 1])

with col1:
    match_id = st.text_input("Match ID (optional)", value="")

with col2:
    player_not_found = st.toggle("Manual Player ID Entry", value=False)

selected_player_ids = []
selected_player_names = []
selected_gender = "All"

if df_players is not None and not player_not_found:
    # Create two columns for Country and Gender filters
    filter_col1, filter_col2 = st.columns([1, 1])
    
    with filter_col1:
        countries = sorted([c for c in df_players["Country"].dropna().unique().tolist() if str(c).strip() != ""])
        if not countries:
            st.error("No countries found in uploaded file.")
            st.stop()
        selected_country = st.selectbox("Select Country", options=countries)
    
    with filter_col2:
        gender_options = ["All", "Men", "Women"]
        selected_gender = st.selectbox("Select Gender", options=gender_options)

    # Filter by country first
    filtered = df_players[df_players["Country"] == selected_country].copy()
    
    # Then filter by gender if not "All"
    if selected_gender != "All":
        if "Gender" in filtered.columns:
            filtered = filtered[filtered["Gender"].str.lower() == selected_gender.lower()]
        else:
            # If Gender column doesn't exist, try to detect from player data
            st.warning("Gender column not found in uploaded file. Attempting to auto-detect...")
    
    filtered = filtered.sort_values("Player Name")

    if filtered.empty:
        st.error(f"No {selected_gender if selected_gender != 'All' else ''} players found for {selected_country}.")
        st.stop()

    # Multi-select for players
    st.subheader("Select Players")
    
    # Option to select all
    select_all = st.checkbox("Select All Players", value=False)
    
    if select_all:
        selected_names = filtered["Player Name"].tolist()
    else:
        selected_names = st.multiselect(
            "Choose one or more players (searchable)",
            options=filtered["Player Name"].tolist(),
            default=[]
        )
    
    if selected_names:
        for name in selected_names:
            player_row = filtered.loc[filtered["Player Name"] == name].iloc[0]
            pid = int(player_row["player_id"])
            selected_player_ids.append(pid)
            selected_player_names.append(name)
        
        st.info(f"📋 Selected {len(selected_player_ids)} player(s)")
        
        # Show selected players
        with st.expander("View Selected Players"):
            for i, (pid, name) in enumerate(zip(selected_player_ids, selected_player_names), 1):
                st.write(f"{i}. {name} (ID: {pid})")

if player_not_found:
    st.subheader("Manual Player Entry")
    
    manual_col1, manual_col2 = st.columns([1, 1])
    
    with manual_col1:
        manual_ids_text = st.text_area(
            "Enter Player IDs (one per line or comma-separated)",
            placeholder="253802\n277916\n232729",
            height=100
        )
    
    with manual_col2:
        manual_gender = st.selectbox("Select Gender for Manual Entry", options=["Men", "Women"])
        selected_gender = manual_gender
    
    if manual_ids_text.strip():
        # Parse player IDs
        manual_ids_text = manual_ids_text.replace(",", "\n")
        manual_ids = []
        for line in manual_ids_text.strip().split("\n"):
            line = line.strip()
            if line.isdigit():
                manual_ids.append(int(line))
        
        if manual_ids:
            selected_player_ids = manual_ids
            selected_player_names = [f"Player_{pid}" for pid in manual_ids]
            st.info(f"📋 Entered {len(selected_player_ids)} player ID(s)")

# Process button
if selected_player_ids:
    col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
    with col_btn2:
        process_button = st.button(
            f"🚀 Process {len(selected_player_ids)} Player(s)", 
            type="primary",
            use_container_width=True,
            disabled=st.session_state.processing
        )
    
    if process_button:
        st.session_state.processing = True
        st.session_state.results = []
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        results = []
        total_players = len(selected_player_ids)
        
        for idx, (player_id, player_name) in enumerate(zip(selected_player_ids, selected_player_names), 1):
            status_text.text(f"Processing {idx}/{total_players}: {player_name} (ID: {player_id})...")
            progress_bar.progress(idx / total_players)
            
            # Determine gender for this player
            player_gender = selected_gender if selected_gender != "All" else "Men"
            
            result = process_single_player(
                player_id=player_id,
                player_name=player_name,
                match_id=match_id.strip() if match_id else "",
                df_players=df_players,
                gender=player_gender
            )
            results.append(result)
            
            # Small delay between requests
            time.sleep(0.5)
        
        st.session_state.results = results
        st.session_state.processing = False
        status_text.empty()
        progress_bar.empty()
        
        st.success(f"✅ Processing complete! Generated {len([r for r in results if r['status'] == 'success'])} file(s)")

# Display results
if st.session_state.results:
    st.divider()
    st.subheader("📥 Download Results")
    
    success_count = len([r for r in st.session_state.results if r["status"] == "success"])
    error_count = len([r for r in st.session_state.results if r["status"] == "error"])
    
    col_stat1, col_stat2, col_stat3 = st.columns(3)
    with col_stat1:
        st.metric("Total Processed", len(st.session_state.results))
    with col_stat2:
        st.metric("Successful", success_count)
    with col_stat3:
        st.metric("Failed", error_count)
    
    # Download All button
    if success_count > 0:
        st.divider()
        col_zip1, col_zip2, col_zip3 = st.columns([1, 2, 1])
        with col_zip2:
            zip_data = create_zip_file(st.session_state.results)
            st.download_button(
                label="📦 Download All as ZIP",
                data=zip_data,
                file_name=f"cricinfo_players_{time.strftime('%Y%m%d_%H%M%S')}.zip",
                mime="application/zip",
                type="primary",
                use_container_width=True
            )
    
    st.divider()
    
    # Display download buttons for successful results
    for result in st.session_state.results:
        if result["status"] == "success":
            col1, col2 = st.columns([3, 1])
            with col1:
                gender_emoji = "👩" if result.get("gender", "Men").lower() == "women" else "👨"
                st.write(f"✅ {gender_emoji} **{result['player_name']}** (ID: {result['player_id']})")
            with col2:
                filename = f"{result['player_id']}_{safe_filename(result['player_name'])}.xlsx"
                st.download_button(
                    label="📥 Download",
                    data=result["data"],
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"download_{result['player_id']}"
                )
        else:
            st.error(f"❌ **{result['player_name']}** (ID: {result['player_id']}) - Error: {result['error']}")
    
    # Clear results button
    st.divider()
    if st.button("🔄 Clear Results", type="secondary"):
        st.session_state.results = []
        st.rerun()

# Footer
st.divider()
st.caption("💡 Tip: Select gender filter to extract Men's or Women's cricket stats. Use 'Download All as ZIP' to get all files at once.")