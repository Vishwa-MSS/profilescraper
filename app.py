import re
import time
import zipfile
from io import BytesIO
from urllib.parse import urljoin

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup


# =========================================================
# STRICT HEADERS PROVIDED BY USER
# =========================================================
CORE_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6",
    "cache-control": "max-age=0",
    "upgrade-insecure-requests": "1",
}

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    )
}

HEADERS = {**DEFAULT_HEADERS, **CORE_HEADERS}


# =========================================================
# CONFIG
# =========================================================
MALE_FORMATS = {
    "Test": 1,
    "ODI": 2,
    "T20I": 3,
    "First-class": 4,
    "List A": 5,
    "T20": 6,
}

FEMALE_FORMATS = {
    "Test": 8,
    "ODI": 9,
    "T20I": 10,
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
CORE_API_URL = "http://new.core.espnuk.org/v2/sports/cricket/athletes/{player_id}"

PLAYER_REQUIRED_COLUMNS = [
    "player_id",
    "Player Name",
    "Country",
    "Gender",
    "Full Name",
    "Born",
    "Age",
    "Batting Style",
    "Bowling Style",
    "Batting Hand",
    "Bowling Hand",
    "Playing Role",
    "Height",
    "Education",
    "Nicknames",
    "Fielding Position",
    "Also Known As",
    "Other",
    "Died",
]


# =========================================================
# HELPERS
# =========================================================
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def is_blank(v) -> bool:
    return pd.isna(v) or str(v).strip() == "" or str(v).strip().lower() == "nan"


def normalize_gender(gender_value: str) -> str:
    g = str(gender_value).strip()
    mapping = {
        "M": "Male",
        "F": "Female",
        "Male": "Male",
        "Female": "Female",
        "male": "Male",
        "female": "Female",
    }
    return mapping.get(g, g)


def ensure_player_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = normalize_columns(df)

    lower_to_actual = {str(c).strip().lower(): c for c in df.columns}
    rename_map = {}

    alias_map = {
        "gender": "Gender",
        "player name": "Player Name",
        "country": "Country",
        "full name": "Full Name",
        "born": "Born",
        "age": "Age",
        "batting style": "Batting Style",
        "bowling style": "Bowling Style",
        "playing role": "Playing Role",
        "height": "Height",
        "education": "Education",
        "nicknames": "Nicknames",
        "fielding position": "Fielding Position",
        "also known as": "Also Known As",
        "other": "Other",
        "died": "Died",
        "batting hand": "Batting Hand",
        "bowling hand": "Bowling Hand",
    }

    for low_name, target_name in alias_map.items():
        if low_name in lower_to_actual and target_name not in df.columns:
            rename_map[lower_to_actual[low_name]] = target_name

    if rename_map:
        df = df.rename(columns=rename_map)

    for col in PLAYER_REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce")
    df = df.dropna(subset=["player_id"]).copy()
    df["player_id"] = df["player_id"].astype(int)

    for col in df.columns:
        if col != "player_id":
            df[col] = df[col].fillna("").astype(str).str.strip()

    df["Gender"] = df["Gender"].map(normalize_gender)

    return df


def read_players_file(uploaded_file) -> pd.DataFrame:
    fname = uploaded_file.name.lower()

    if fname.endswith(".csv"):
        df = pd.read_csv(uploaded_file, encoding="utf-8-sig")
    else:
        df = pd.read_excel(uploaded_file)

    return ensure_player_columns(df)


def write_updated_file_same_format(df: pd.DataFrame, original_name: str):
    if original_name.lower().endswith(".csv"):
        data = df.to_csv(index=False).encode("utf-8-sig")
        return data, original_name, "text/csv"

    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Players")
    return out.getvalue(), original_name, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def safe_sheet_name(name: str) -> str:
    for ch in ['\\', '/', '*', '[', ']', ':', '?']:
        name = name.replace(ch, "_")
    return name[:31]


def make_safe_filename(name: str) -> str:
    name = str(name).strip()
    name = re.sub(r'[\\/*?:"<>|]+', "_", name)
    name = re.sub(r"\s+", "_", name)
    return name[:150] if name else "player"


def get_formats_for_gender(gender_value: str) -> dict:
    if normalize_gender(gender_value).lower() == "female":
        return FEMALE_FORMATS
    return MALE_FORMATS


# =========================================================
# CORE API
# =========================================================
def fetch_player_core_info(player_id: int, session: requests.Session) -> dict:
    url = CORE_API_URL.format(player_id=int(player_id))

    try:
        resp = session.get(
            url,
            headers=CORE_HEADERS,
            timeout=25,
            allow_redirects=True
        )
        resp.raise_for_status()
        data = resp.json()

        gender = normalize_gender(data.get("gender", ""))

        batting_style = ""
        bowling_style = ""
        batting_hand = ""
        bowling_hand = ""

        styles = data.get("style") or data.get("styles") or []
        for s in styles:
            style_type = str(s.get("type", "")).strip().lower()
            description = str(s.get("description", "")).strip()

            if style_type == "batting":
                batting_style = description
                if "right" in description.lower():
                    batting_hand = "Right"
                elif "left" in description.lower():
                    batting_hand = "Left"

            elif style_type == "bowling":
                bowling_style = description
                if "right" in description.lower():
                    bowling_hand = "Right"
                elif "left" in description.lower():
                    bowling_hand = "Left"

        return {
            "Player Name": str(data.get("displayName", "")).strip() or str(data.get("name", "")).strip(),
            "Full Name": str(data.get("fullName", "")).strip(),
            "Gender": gender,
            "Age": str(data.get("age", "")).strip(),
            "Born": str(data.get("dateOfBirthStr", "")).strip(),
            "Batting Style": batting_style,
            "Bowling Style": bowling_style,
            "Batting Hand": batting_hand,
            "Bowling Hand": bowling_hand,
        }
    except Exception:
        return {}


# =========================================================
# PROFILE SCRAPE
# =========================================================
def scrape_player_profile(player_id: int, session: requests.Session) -> dict:
    url = PROFILE_URL.format(player_id=player_id)
    resp = session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    name = ""
    h1 = soup.find("h1")
    if h1:
        name = h1.get_text(" ", strip=True)

    page_text = soup.get_text(" ", strip=True).lower()

    inferred_gender = ""
    if "women" in page_text or "female" in page_text:
        inferred_gender = "Female"
    elif "men" in page_text or "male" in page_text:
        inferred_gender = "Male"

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
        "Gender": inferred_gender,
    }


# =========================================================
# PLAYER FILE HELPERS
# =========================================================
def build_player_info_row_from_file(df_players: pd.DataFrame, player_id: int) -> dict:
    if df_players is None or df_players.empty:
        return {}

    row = df_players.loc[df_players["player_id"] == int(player_id)]
    if row.empty:
        return {}

    row = row.iloc[0].to_dict()
    result = {k: row.get(k, "") for k in PLAYER_REQUIRED_COLUMNS if k != "player_id"}
    result["Gender"] = normalize_gender(result.get("Gender", ""))
    return result


def update_players_df_row(df_players: pd.DataFrame, player_id: int, info: dict) -> pd.DataFrame:
    df_players = ensure_player_columns(df_players)
    pid = int(player_id)

    if (df_players["player_id"] == pid).any():
        idx = df_players.index[df_players["player_id"] == pid][0]
    else:
        new_row = {col: "" for col in PLAYER_REQUIRED_COLUMNS}
        new_row["player_id"] = pid
        df_players = pd.concat([df_players, pd.DataFrame([new_row])], ignore_index=True)
        idx = df_players.index[-1]

    for key, val in info.items():
        if key not in df_players.columns:
            continue
        if is_blank(val):
            continue

        val = str(val).strip()

        if key == "Gender":
            df_players.at[idx, key] = normalize_gender(val)
        else:
            current = df_players.at[idx, key]
            if is_blank(current):
                df_players.at[idx, key] = val

    return df_players


# =========================================================
# TABLE SCRAPING
# =========================================================
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
        except Exception:
            continue

        df = pick_best_table(tables, view_name)
        if df is None:
            continue

        df = clean_dataframe(df)
        if df.empty:
            continue

        df["source_url"] = page_url
        all_parts.append(df)

    if not all_parts:
        return pd.DataFrame()

    combined = pd.concat(all_parts, ignore_index=True)
    combined = combined.drop_duplicates().reset_index(drop=True)
    return combined


# =========================================================
# PLAYER INFO COLLECTION
# =========================================================
def collect_player_info(player_id: int, df_players: pd.DataFrame, session: requests.Session) -> dict:
    base_info = build_player_info_row_from_file(df_players, player_id)

    core_info = {}
    if is_blank(base_info.get("Gender", "")) or is_blank(base_info.get("Player Name", "")):
        core_info = fetch_player_core_info(player_id, session)
        time.sleep(0.4)

    merged_probe = {}
    merged_probe.update(base_info)
    merged_probe.update({k: v for k, v in core_info.items() if not is_blank(v)})

    scraped_info = {}
    need_profile = any(
        is_blank(merged_probe.get(k, ""))
        for k in ["Full Name", "Born", "Batting Style", "Bowling Style", "Playing Role", "Gender"]
    )

    if need_profile:
        try:
            scraped_info = scrape_player_profile(player_id, session)
            time.sleep(0.4)
        except Exception:
            scraped_info = {}

    player_info = {}
    player_info.update(scraped_info)
    player_info.update(core_info)
    player_info.update({k: v for k, v in base_info.items() if not is_blank(v)})

    if is_blank(player_info.get("Gender", "")):
        if not is_blank(core_info.get("Gender", "")):
            player_info["Gender"] = core_info.get("Gender", "")
        elif not is_blank(scraped_info.get("Gender", "")):
            player_info["Gender"] = scraped_info.get("Gender", "")

    player_info["Gender"] = normalize_gender(player_info.get("Gender", ""))

    for col in PLAYER_REQUIRED_COLUMNS:
        if col != "player_id":
            player_info.setdefault(col, "")

    return player_info


# =========================================================
# MAIN SINGLE PLAYER
# =========================================================
def generate_excel_and_update_uploaded_file(player_id: int, match_id: str, df_players: pd.DataFrame):
    session = requests.Session()
    session.headers.update(HEADERS)

    df_players = ensure_player_columns(df_players)

    player_info = collect_player_info(player_id, df_players, session)

    df_players = update_players_df_row(df_players, player_id, player_info)

    updated_row = build_player_info_row_from_file(df_players, player_id)
    gender_value = normalize_gender(updated_row.get("Gender", "") or player_info.get("Gender", ""))

    formats_to_use = get_formats_for_gender(gender_value)

    player_info_sheet_cols = [
        "Player Name", "Country", "Match ID",
        "Full Name", "Born", "Age", "Gender",
        "Batting Style", "Bowling Style", "Batting Hand", "Bowling Hand",
        "Playing Role", "Height", "Education", "Nicknames",
        "Fielding Position", "Also Known As", "Other", "Died"
    ]

    player_info_row = {c: "" for c in player_info_sheet_cols}
    player_info_row.update(updated_row)
    player_info_row["Gender"] = gender_value
    player_info_row["Match ID"] = match_id or ""
    player_info_df = pd.DataFrame([player_info_row], columns=player_info_sheet_cols)

    index_rows = []
    all_data_parts = []

    out_xlsx = BytesIO()
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        player_info_df.to_excel(writer, sheet_name="PLAYER_INFO", index=False)

        for format_name, class_id in formats_to_use.items():
            for view_name, params in VIEWS.items():
                url = BASE_URL.format(player_id=player_id, class_id=class_id, params=params)

                try:
                    df = fetch_table_for_url(url, session, view_name)

                    if df.empty:
                        index_rows.append({
                            "Format": format_name,
                            "Class ID": class_id,
                            "Gender": gender_value,
                            "View": view_name,
                            "Rows": 0,
                            "URL": url,
                            "Sheet": ""
                        })
                        continue

                    df.insert(0, "Player ID", int(player_id))
                    df.insert(1, "Gender", gender_value)
                    df.insert(2, "Format", format_name)
                    df.insert(3, "Class ID", class_id)
                    df.insert(4, "View", view_name)

                    sheet_name = safe_sheet_name(f"{format_name}_{view_name}")
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    all_data_parts.append(df)

                    index_rows.append({
                        "Format": format_name,
                        "Class ID": class_id,
                        "Gender": gender_value,
                        "View": view_name,
                        "Rows": len(df),
                        "URL": url,
                        "Sheet": sheet_name
                    })

                except Exception as e:
                    index_rows.append({
                        "Format": format_name,
                        "Class ID": class_id,
                        "Gender": gender_value,
                        "View": view_name,
                        "Rows": 0,
                        "URL": url,
                        "Sheet": "",
                        "Error": str(e)
                    })

        if all_data_parts:
            pd.concat(all_data_parts, ignore_index=True).to_excel(writer, sheet_name="ALL_DATA", index=False)

        pd.DataFrame(index_rows).to_excel(writer, sheet_name="INDEX", index=False)

    return out_xlsx.getvalue(), df_players, updated_row, gender_value


# =========================================================
# BULK ZIP
# =========================================================
def generate_bulk_zip_by_filters(
    df_players: pd.DataFrame,
    match_id: str = "",
    selected_country: str = "",
    selected_gender: str = ""
):
    df_players = ensure_player_columns(df_players)

    filtered = df_players.copy()

    if selected_country and selected_country != "All":
        filtered = filtered[
            filtered["Country"].fillna("").astype(str).str.strip() == selected_country
        ].copy()

    if selected_gender and selected_gender != "All":
        filtered = filtered[
            filtered["Gender"].fillna("").astype(str).map(normalize_gender) == normalize_gender(selected_gender)
        ].copy()

    filtered = filtered.drop_duplicates(subset=["player_id"]).copy()
    filtered = filtered[filtered["player_id"].notna()].copy()

    if filtered.empty:
        raise ValueError("No players found for selected country/gender filter.")

    zip_buffer = BytesIO()
    updated_players_df = df_players.copy()
    summary_rows = []

    progress = st.progress(0)
    status = st.empty()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        total = len(filtered)

        for i, (_, row) in enumerate(filtered.iterrows(), start=1):
            player_id = int(row["player_id"])
            player_name = str(row.get("Player Name", "")).strip() or f"player_{player_id}"
            country = str(row.get("Country", "")).strip()
            input_gender = normalize_gender(row.get("Gender", ""))

            status.text(f"Processing {i}/{total}: {player_name} ({player_id})")

            try:
                excel_data, updated_players_df, updated_row, resolved_gender = generate_excel_and_update_uploaded_file(
                    player_id=player_id,
                    match_id=match_id.strip(),
                    df_players=updated_players_df
                )

                filename = f"{make_safe_filename(country)}_{make_safe_filename(player_name)}_{player_id}.xlsx"
                zipf.writestr(filename, excel_data)

                summary_rows.append({
                    "player_id": player_id,
                    "Player Name": player_name,
                    "Country": country,
                    "Input Gender": input_gender,
                    "Resolved Gender": resolved_gender,
                    "Status": "Success"
                })

            except Exception as e:
                summary_rows.append({
                    "player_id": player_id,
                    "Player Name": player_name,
                    "Country": country,
                    "Input Gender": input_gender,
                    "Resolved Gender": "",
                    "Status": f"Failed: {str(e)}"
                })

            progress.progress(i / total)

        status.text("Preparing updated player file and summary...")

        updated_file_bytes, updated_file_name, _ = write_updated_file_same_format(
            updated_players_df, "updated_players.xlsx"
        )
        zipf.writestr(updated_file_name, updated_file_bytes)

        summary_df = pd.DataFrame(summary_rows)
        zipf.writestr("bulk_summary.csv", summary_df.to_csv(index=False).encode("utf-8-sig"))

        progress.progress(1.0)
        status.text("Bulk ZIP ready.")

    zip_buffer.seek(0)
    return zip_buffer.getvalue(), updated_players_df, pd.DataFrame(summary_rows)


# =========================================================
# STREAMLIT UI
# =========================================================
st.set_page_config(page_title="Cricinfo Player Stats Downloader", layout="wide")
st.title("Cricinfo Player Stats Downloader")

uploaded = st.file_uploader(
    "Upload Players file (csv/xlsx/xls). Updated uploaded file will be returned in same format.",
    type=["csv", "xlsx", "xls"]
)

df_players = None
uploaded_name = "players.csv"

if uploaded:
    try:
        df_players = read_players_file(uploaded)
        uploaded_name = uploaded.name
        st.success(f"Loaded {len(df_players)} rows from {uploaded_name}")
    except Exception as e:
        st.error(f"Could not read uploaded file: {e}")
        st.stop()

col1, col2, col3 = st.columns([1.1, 1.4, 1.4])

with col1:
    match_id = st.text_input("Match ID (optional)", value="")

with col2:
    mode = st.radio("Mode", ["Single Player", "Bulk by Country + Gender"], horizontal=False)

with col3:
    player_not_found = st.toggle("Player name not found / use manual Player ID", value=False)

selected_player_id = None
bulk_country = "All"
bulk_gender = "All"

if df_players is not None:
    df_players["Country"] = df_players["Country"].fillna("").astype(str).str.strip()
    df_players["Player Name"] = df_players["Player Name"].fillna("").astype(str).str.strip()
    df_players["Gender"] = df_players["Gender"].fillna("").astype(str).map(normalize_gender)

if df_players is not None and mode == "Single Player" and not player_not_found:
    countries = sorted(
        [c for c in df_players["Country"].drop_duplicates().tolist() if str(c).strip() != ""]
    )
    genders = sorted(
        [g for g in df_players["Gender"].drop_duplicates().tolist() if str(g).strip() != ""]
    )

    c1, c2 = st.columns(2)

    with c1:
        selected_country = st.selectbox("Select Country", options=["All"] + countries)

    with c2:
        selected_gender = st.selectbox("Select Gender", options=["All"] + genders)

    filtered = df_players.copy()

    if selected_country != "All":
        filtered = filtered[filtered["Country"] == selected_country].copy()

    if selected_gender != "All":
        filtered = filtered[filtered["Gender"] == selected_gender].copy()

    filtered = filtered.sort_values(["Player Name", "player_id"])

    player_options = [
        f"{row['Player Name']} ({int(row['player_id'])})"
        for _, row in filtered.iterrows()
        if str(row["Player Name"]).strip() != ""
    ]

    if player_options:
        selected_option = st.selectbox("Select Player Name", options=player_options)
        match = re.search(r"\((\d+)\)\s*$", selected_option)
        if match:
            selected_player_id = int(match.group(1))
            st.caption(f"Selected Player ID: {selected_player_id}")
    else:
        st.warning("No players found for selected country/gender filter. Use manual Player ID.")

elif df_players is not None and mode == "Bulk by Country + Gender":
    countries = sorted(
        [c for c in df_players["Country"].drop_duplicates().tolist() if str(c).strip() != ""]
    )
    genders = sorted(
        [g for g in df_players["Gender"].drop_duplicates().tolist() if str(g).strip() != ""]
    )

    c1, c2 = st.columns(2)

    with c1:
        bulk_country = st.selectbox("Bulk Filter - Country", options=["All"] + countries)

    with c2:
        bulk_gender = st.selectbox("Bulk Filter - Gender", options=["All"] + genders)

    preview_df = df_players.copy()

    if bulk_country != "All":
        preview_df = preview_df[preview_df["Country"] == bulk_country].copy()

    if bulk_gender != "All":
        preview_df = preview_df[preview_df["Gender"] == bulk_gender].copy()

    st.info(f"{len(preview_df)} players matched for bulk scraping.")
    preview_cols = [c for c in ["player_id", "Player Name", "Country", "Gender"] if c in preview_df.columns]
    st.dataframe(preview_df[preview_cols].head(50), use_container_width=True)

if player_not_found or df_players is None:
    selected_player_id = st.number_input("Enter Player ID", min_value=1, step=1, value=1212830)
    manual_name = st.text_input("Player Name (optional)", value="")
    manual_country = st.text_input("Country (optional)", value="")
    manual_gender = st.selectbox("Gender (optional)", options=["", "Male", "Female"])

    if df_players is None:
        df_players = pd.DataFrame(columns=PLAYER_REQUIRED_COLUMNS)

    df_players = ensure_player_columns(df_players)

    if selected_player_id and not (df_players["player_id"] == int(selected_player_id)).any():
        new_row = {col: "" for col in PLAYER_REQUIRED_COLUMNS}
        new_row["player_id"] = int(selected_player_id)
        new_row["Player Name"] = manual_name.strip()
        new_row["Country"] = manual_country.strip()
        new_row["Gender"] = normalize_gender(manual_gender.strip())
        df_players = pd.concat([df_players, pd.DataFrame([new_row])], ignore_index=True)

go = st.button("Go", type="primary")

if go:
    if mode == "Single Player":
        if not selected_player_id:
            st.error("Please select a player or enter a Player ID.")
            st.stop()

        with st.spinner("Updating uploaded file and collecting player stats..."):
            try:
                excel_data, updated_players_df, updated_row, gender_value = generate_excel_and_update_uploaded_file(
                    player_id=int(selected_player_id),
                    match_id=match_id.strip(),
                    df_players=df_players
                )
            except Exception as e:
                st.error(f"Failed: {e}")
                st.stop()

        st.success("Done")

        st.subheader("Updated Player Row")
        st.dataframe(pd.DataFrame([updated_row]), use_container_width=True)

        st.write(f"**Resolved Gender:** {gender_value or 'Unknown'}")

        st.download_button(
            label="Download Excel Stats",
            data=excel_data,
            file_name=f"{int(selected_player_id)}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        updated_file_bytes, updated_file_name, updated_file_mime = write_updated_file_same_format(
            updated_players_df, uploaded_name
        )

        st.download_button(
            label="Download Updated Uploaded File",
            data=updated_file_bytes,
            file_name=updated_file_name,
            mime=updated_file_mime
        )

    elif mode == "Bulk by Country + Gender":
        if df_players is None or df_players.empty:
            st.error("Please upload a valid players file.")
            st.stop()

        with st.spinner("Running bulk scraping and preparing ZIP..."):
            try:
                zip_data, updated_players_df, summary_df = generate_bulk_zip_by_filters(
                    df_players=df_players,
                    match_id=match_id.strip(),
                    selected_country=bulk_country,
                    selected_gender=bulk_gender
                )
            except Exception as e:
                st.error(f"Bulk scraping failed: {e}")
                st.stop()

        st.success("Bulk scraping completed.")

        st.subheader("Bulk Summary")
        st.dataframe(summary_df, use_container_width=True)

        zip_name_parts = ["players_bulk"]
        if bulk_country != "All":
            zip_name_parts.append(make_safe_filename(bulk_country))
        if bulk_gender != "All":
            zip_name_parts.append(make_safe_filename(bulk_gender))

        zip_name = "_".join(zip_name_parts) + ".zip"

        st.download_button(
            label="Download Bulk ZIP",
            data=zip_data,
            file_name=zip_name,
            mime="application/zip"
        )