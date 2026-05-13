import streamlit as st
import pandas as pd
from rapidfuzz import fuzz, process
import re
from datetime import datetime
import numpy as np

def extract_date_from_born(born_text):
    """Extract date from the 'Born' column which contains date and place"""
    if pd.isna(born_text):
        return None
    
    # Regular expression to match various date formats
    patterns = [
        r'(\w+ \d{1,2}, \d{4})',  # Month DD, YYYY
        r'(\d{1,2}-\d{1,2}-\d{4})',  # DD-MM-YYYY
        r'(\d{4}-\d{1,2}-\d{1,2})',  # YYYY-MM-DD
    ]
    
    for pattern in patterns:
        match = re.search(pattern, str(born_text))
        if match:
            return match.group(1)
    
    return None

def normalize_date(date_str):
    """Convert various date formats to a standard format DD-MM-YYYY"""
    if pd.isna(date_str) or date_str is None:
        return None
    
    date_str = str(date_str).strip()
    
    date_formats = [
        '%B %d, %Y',  # January 08, 1986
        '%d-%m-%Y',   # 23-11-1991
        '%m-%d-%Y',   # 11-23-1991
        '%Y-%m-%d',   # 1991-11-23
        '%d/%m/%Y',   # 23/11/1991
        '%m/%d/%Y',   # 11/23/1991
    ]
    
    for fmt in date_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%d-%m-%Y')
        except ValueError:
            continue
    
    return None

def match_players_optimized(advance_df, gender_df, threshold=85):
    """Optimized matching using vectorized operations and rapid fuzzy matching"""
    
    # Extract and normalize dates
    st.write("📅 Extracting birth dates from 'Born' column...")
    advance_df = advance_df.copy()
    advance_df['extracted_date'] = advance_df['Born'].apply(extract_date_from_born)
    advance_df['normalized_date'] = advance_df['extracted_date'].apply(normalize_date)
    
    st.write("📅 Normalizing dates in gender file...")
    gender_df = gender_df.copy()
    gender_df['normalized_date'] = gender_df['dateofbirth'].apply(normalize_date)
    
    # Remove rows with no date
    advance_with_dates = advance_df[advance_df['normalized_date'].notna()].copy()
    advance_no_dates = advance_df[advance_df['normalized_date'].isna()].copy()
    
    gender_with_dates = gender_df[gender_df['normalized_date'].notna()].copy()
    
    st.write(f"✅ Found {len(advance_with_dates)} players with valid dates")
    st.write(f"⚠️ Found {len(advance_no_dates)} players without valid dates (will be in 'others')")
    
    # Create a dictionary for quick lookup
    st.write("🔍 Creating gender lookup dictionary...")
    gender_dict = {}
    for _, row in gender_with_dates.iterrows():
        date_key = row['normalized_date']
        if date_key not in gender_dict:
            gender_dict[date_key] = []
        gender_dict[date_key].append({
            'gender': row['gender'],
            'dateofbirth': row['dateofbirth'],
            'fullname': row.get('fullname', '')
        })
    
    # Prepare for matching
    matched_men = []
    matched_women = []
    unmatched = []
    
    # Get unique dates from gender file for faster matching
    gender_dates = list(gender_dict.keys())
    
    st.write(f"🚀 Matching {len(advance_with_dates)} players against {len(gender_dates)} unique dates...")
    progress_bar = st.progress(0)
    
    # Process in batches for better performance
    batch_size = 100
    total_batches = (len(advance_with_dates) + batch_size - 1) // batch_size
    
    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(advance_with_dates))
        batch = advance_with_dates.iloc[start_idx:end_idx]
        
        for _, adv_row in batch.iterrows():
            player_date = adv_row['normalized_date']
            
            # First try exact match
            if player_date in gender_dict:
                match_info = gender_dict[player_date][0]  # Take first match
                row_with_gender = adv_row.copy()
                row_with_gender['matched_gender'] = match_info['gender']
                row_with_gender['matched_dateofbirth'] = match_info['dateofbirth']
                row_with_gender['match_score'] = 100
                
                if match_info['gender'].lower() == 'm':
                    matched_men.append(row_with_gender)
                elif match_info['gender'].lower() == 'f':
                    matched_women.append(row_with_gender)
                else:
                    unmatched.append(row_with_gender)
            else:
                # Try fuzzy match only if exact match fails
                best_match = process.extractOne(
                    player_date, 
                    gender_dates, 
                    scorer=fuzz.ratio,
                    score_cutoff=threshold
                )
                
                if best_match:
                    matched_date = best_match[0]
                    match_score = best_match[1]
                    match_info = gender_dict[matched_date][0]
                    
                    row_with_gender = adv_row.copy()
                    row_with_gender['matched_gender'] = match_info['gender']
                    row_with_gender['matched_dateofbirth'] = match_info['dateofbirth']
                    row_with_gender['match_score'] = match_score
                    
                    if match_info['gender'].lower() == 'm':
                        matched_men.append(row_with_gender)
                    elif match_info['gender'].lower() == 'f':
                        matched_women.append(row_with_gender)
                    else:
                        unmatched.append(row_with_gender)
                else:
                    unmatched.append(adv_row)
        
        progress_bar.progress((batch_idx + 1) / total_batches)
    
    progress_bar.empty()
    
    # Add players without dates to unmatched
    for _, row in advance_no_dates.iterrows():
        unmatched.append(row)
    
    # Convert lists to DataFrames
    men_df = pd.DataFrame(matched_men) if matched_men else pd.DataFrame()
    women_df = pd.DataFrame(matched_women) if matched_women else pd.DataFrame()
    others_df = pd.DataFrame(unmatched) if unmatched else pd.DataFrame()
    
    return men_df, women_df, others_df

def main():
    st.set_page_config(page_title="Player Gender Matcher", page_icon="⚽", layout="wide")
    
    st.title("🏏 Cricket Player Gender Matcher (Optimized)")
    st.markdown("""
    This application matches players from the **advance_playing_xi** file with gender data 
    using **optimized fuzzy matching** on birth dates. It generates three CSV files:
    - **Men**: Matched male players
    - **Women**: Matched female players  
    - **Others**: Unmatched or incomplete data
    
    ⚡ **Optimized for speed** with batch processing and rapid fuzzy matching!
    """)
    
    st.sidebar.header("Upload CSV Files")
    
    # File uploaders
    advance_file = st.sidebar.file_uploader("Upload advance_playing_xi CSV", type=['csv'])
    gender_file = st.sidebar.file_uploader("Upload gender CSV", type=['csv'])
    
    # Matching threshold
    threshold = st.sidebar.slider("Fuzzy Match Threshold (%)", min_value=70, max_value=100, value=85, step=5)
    
    if advance_file is not None and gender_file is not None:
        try:
            # Load CSVs
            with st.spinner("Loading CSV files..."):
                advance_df = pd.read_csv(advance_file)
                gender_df = pd.read_csv(gender_file)
            
            st.success(f"✅ Loaded {len(advance_df):,} players from advance_playing_xi")
            st.success(f"✅ Loaded {len(gender_df):,} records from gender file")
            
            # Display sample data
            with st.expander("📊 Preview advance_playing_xi data (first 10 rows)"):
                st.dataframe(advance_df.head(10))
            
            with st.expander("📊 Preview gender data (first 10 rows)"):
                st.dataframe(gender_df.head(10))
            
            # Process button
            if st.button("🚀 Start Matching", type="primary"):
                st.header("Processing...")
                
                start_time = datetime.now()
                
                men_df, women_df, others_df = match_players_optimized(advance_df, gender_df, threshold)
                
                end_time = datetime.now()
                processing_time = (end_time - start_time).total_seconds()
                
                st.success(f"✅ Matching complete in {processing_time:.2f} seconds!")
                
                # Display results
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("👨 Men Matched", f"{len(men_df):,}")
                with col2:
                    st.metric("👩 Women Matched", f"{len(women_df):,}")
                with col3:
                    st.metric("❓ Others/Unmatched", f"{len(others_df):,}")
                
                # Display and download results
                st.header("📥 Download Results")
                
                col1, col2, col3 = st.columns(3)
                
                # Men CSV
                with col1:
                    st.subheader("Men")
                    if not men_df.empty:
                        st.dataframe(men_df.head(10))
                        csv_men = men_df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label=f"⬇️ Download Men.csv ({len(men_df):,} records)",
                            data=csv_men,
                            file_name='men.csv',
                            mime='text/csv',
                            key='men'
                        )
                    else:
                        st.info("No men matched")
                
                # Women CSV
                with col2:
                    st.subheader("Women")
                    if not women_df.empty:
                        st.dataframe(women_df.head(10))
                        csv_women = women_df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label=f"⬇️ Download Women.csv ({len(women_df):,} records)",
                            data=csv_women,
                            file_name='women.csv',
                            mime='text/csv',
                            key='women'
                        )
                    else:
                        st.info("No women matched")
                
                # Others CSV
                with col3:
                    st.subheader("Others/Unmatched")
                    if not others_df.empty:
                        st.dataframe(others_df.head(10))
                        csv_others = others_df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label=f"⬇️ Download Others.csv ({len(others_df):,} records)",
                            data=csv_others,
                            file_name='others.csv',
                            mime='text/csv',
                            key='others'
                        )
                    else:
                        st.info("No unmatched records")
                
                # Summary statistics
                st.header("📈 Match Statistics")
                
                total_records = len(advance_df)
                matched_records = len(men_df) + len(women_df)
                match_rate = (matched_records / total_records * 100) if total_records > 0 else 0
                
                col1, col2, col3, col4, col5 = st.columns(5)
                
                with col1:
                    st.metric("Total Records", f"{total_records:,}")
                with col2:
                    st.metric("Matched Records", f"{matched_records:,}")
                with col3:
                    st.metric("Match Rate", f"{match_rate:.2f}%")
                with col4:
                    st.metric("Unmatched", f"{len(others_df):,}")
                with col5:
                    st.metric("Processing Time", f"{processing_time:.2f}s")
                
        except Exception as e:
            st.error(f"❌ Error processing files: {str(e)}")
            st.exception(e)
    
    else:
        st.info("👆 Please upload both CSV files to begin matching")
        
        # Instructions
        st.markdown("""
        ### Instructions:
        1. Upload the **advance_playing_xi_with_player_id.csv** file
        2. Upload the **gender.csv** file
        3. Adjust the fuzzy match threshold if needed (default: 85%)
        4. Click **Start Matching** to process
        5. Download the three generated CSV files (men, women, others)
        
        ### Optimizations:
        - ⚡ Uses **rapidfuzz** library (10x faster than fuzzywuzzy)
        - 🔍 **Exact match first**, fuzzy match only when needed
        - 📦 **Batch processing** for better performance
        - 🗂️ **Dictionary lookup** for O(1) access to gender data
        - ⏱️ Processing time displayed after completion
        """)

if __name__ == "__main__":
    main()