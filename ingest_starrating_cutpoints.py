# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the HEDIS files
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

import pandas as pd
import re
from datetime import datetime

volumepath = "/Volumes/mimi_ws_1/partcd/src/starrating" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "partcd" # delta table destination schema

# COMMAND ----------

try:
    parse(None)
except (ParserError, TypeError) as e:
    print(f"Error parsing string: {e}")

# COMMAND ----------



def parse_date_range(date_str):
    """Parse date range string like '02/2025 – 05/2025' into start and end dates
    
    Handles:
    - Any weird separator characters (–, -, â, ñ, etc.) by replacing with spaces
    - Dates without days (e.g., 02/2019) by adding day 1 for start, last day for end
    """
    if pd.isna(date_str) or date_str == 'Not Applicable':
        return None, None
    
    # Clean up the string
    date_str = str(date_str).strip()
    
    # Replace any non-alphanumeric character (except /) with space
    # This catches –, -, â, ñ, and any other weird characters
    import re
    date_str = re.sub(r'[^0-9/a-zA-Z]', ' ', date_str)
    
    # Split on whitespace and filter out empty strings
    parts = [p for p in date_str.split() if p]
    
    if len(parts) >= 2:
        start_date = parts[0]
        end_date = parts[-1]
    elif len(parts) == 1:
        start_date = parts[0]
        end_date = parts[0]
    else:
        return date_str, date_str
    
    # Add missing days if needed
    start_date = add_day_to_date(start_date, position='start')
    end_date = add_day_to_date(end_date, position='end')
    
    try:
        start_date = parse(start_date).date()
        end_date = parse(end_date).date()
    except (ParserError, TypeError) as e:
        print(f"Error parsing string: {e}")

    return start_date, end_date

def add_day_to_date(date_str, position='start'):
    """
    Add day to date if missing.
    
    Examples:
    - 02/2019 (start) -> 02/01/2019
    - 02/2019 (end) -> 02/28/2019 (or 02/29/2019 for leap years)
    - 04/01/2018 -> 04/01/2018 (no change)
    
    Args:
        date_str: Date string (e.g., '02/2019' or '04/01/2018')
        position: 'start' or 'end' - determines which day to use if missing
    """
    if not date_str or date_str == 'None':
        return date_str
    
    parts = date_str.split('/')
    
    # If already has day (3 parts: MM/DD/YYYY), return as-is
    if len(parts) == 3:
        return date_str
    
    # If only MM/YYYY (2 parts), add day
    if len(parts) == 2:
        month = parts[0]
        year = parts[1]
        
        if position == 'start':
            # Start of month: use day 1
            return f"{month}/01/{year}"
        else:
            # End of month: calculate last day
            import calendar
            try:
                last_day = calendar.monthrange(int(year), int(month))[1]
                return f"{month}/{last_day:02d}/{year}"
            except (ValueError, IndexError):
                # If invalid month/year, just use day 1
                return f"{month}/01/{year}"
    
    # If format is unexpected, return as-is
    return date_str

def parse_filename(filepath):
    """
    Parse filename to extract metadata.
    
    Examples:
    - 2023 Star Ratings Data Table - Part C Cut Points (Oct 04 2022).csv
    - 2026_Star_Ratings_Data_Table_-_Part_C_Cut_Points__Oct_8_2025_.csv
    - 2020 Star Ratings Data Table - Part D Cutpoints (Oct 21 2019).csv
    
    Returns: (measurement_year, part_type, generation_date)
    """
    filename = filepath.name
    
    # Extract measurement year (first 4 digits)
    year_match = re.search(r'^(\d{4})', filename)
    measurement_year = int(year_match.group(1)) if year_match else None
    
    # Extract part type (Part C or Part D) - handle both spaces and underscores
    if re.search(r'Part[_\s]C', filename, re.IGNORECASE):
        part_type = 'Part_C'
    elif re.search(r'Part[_\s]D', filename, re.IGNORECASE):
        part_type = 'Part_D'
    else:
        part_type = 'Unknown'
    
    # Extract generation date - handle both parentheses and double underscores
    # Pattern 1: (Oct 04 2022) or (Oct 8 2025)
    date_match = re.search(r'\(([A-Za-z]+)\s+(\d+)\s+(\d{4})\)', filename)
    if not date_match:
        # Pattern 2: __Oct_8_2025_ or similar with underscores
        date_match = re.search(r'_+([A-Za-z]+)[_\s]+(\d+)[_\s]+(\d{4})_*', filename)
    
    if date_match:
        month_str = date_match.group(1)
        day = date_match.group(2).zfill(2)  # Pad single digit days
        year = date_match.group(3)
        
        # Convert month name to number
        month_map = {
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
            'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
            'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
        }
        month = month_map.get(month_str.lower()[:3], '01')
        generation_date = f"{year}-{month}-{day}"
    else:
        generation_date = None

    try:
        generation_date = parse(generation_date).date()
    except (ParserError, TypeError) as e:
        print(f"Error parsing string: {e}")
    
    return measurement_year, part_type, generation_date

def parse_range_value(value_str):
    # 1. Extract all numbers
    numbers = re.findall(r'-?\d+\.?\d*', value_str)
    
    # Helper function to safely convert to float
    def to_float(num_str):
        try:
            return float(num_str)
        except (ValueError, TypeError):
            return None
    
    # 2. If 2+ numbers → Range (first is start, last is end)
    if len(numbers) >= 2:
        return to_float(numbers[0]), to_float(numbers[-1]), value_str
    
    # 3. If 1 number → Check for '<' to determine direction
    elif len(numbers) == 1:
        if '<' in value_str:
            return None, to_float(numbers[0]), value_str  # Upper bound
        else:
            return to_float(numbers[0]), None, value_str  # Lower bound
    else:
        return None, None, value_str

def transform_star_ratings_part_d(df, performance_year):
    """Transform Part D format (with org_type column)"""
    # Row 1: Measure names
    # Row 2: Date ranges
    # Row 3+: Data
    measure_names = df.iloc[1].values
    date_ranges = df.iloc[2].values
    data_rows = df.iloc[3:].reset_index(drop=True)
    
    results = []
    for idx, row in data_rows.iterrows():
        org_type = row.iloc[0]
        star_label = str(row.iloc[1]).lower()
        
        star_match = re.search(r'(\d+)\s?star', star_label)
        if star_match:
            star = int(star_match.group(1))
        else:
            continue
        
        for col_idx in range(2, len(row)):
            measure_name = measure_names[col_idx]
            date_range = date_ranges[col_idx]
            value = row.iloc[col_idx]
            
            if pd.isna(measure_name) or measure_name == '' or pd.isna(value):
                continue
            
            period_start, period_end = parse_date_range(date_range)
            lower_bound, upper_bound, original_text = parse_range_value(value)
            
            result = {
                'performance_year': performance_year,
                'part_type': 'Part_D',
                'org_type': org_type,
                'measure_code': str(measure_name).split(':')[0].strip(),
                'measure_name': str(measure_name).split(':')[-1].strip(),
                'measure_period_start': period_start,
                'measure_period_end': period_end,
                'star': star,
                'lower_bound': lower_bound,
                'upper_bound': upper_bound,
                'original_text': original_text
            }
            
            results.append(result)
    
    return pd.DataFrame(results)

def transform_star_ratings_part_c(df, performance_year):
    """Transform Part C format (no org_type column)"""
    # Row 1: Measure names
    # Row 2: Date ranges
    # Row 3+: Data
    measure_names = df.iloc[1].values
    date_ranges = df.iloc[2].values
    data_rows = df.iloc[3:].reset_index(drop=True)
    
    results = []
    for idx, row in data_rows.iterrows():
        star_label = str(row.iloc[0]).lower()
        star_match = re.search(r'(\d+)\s?star', star_label)
        
        if star_match:
            star = int(star_match.group(1))
        else:
            continue
        # Start from column 1 (no org_type column in Part C)
        for col_idx in range(1, len(row)):
            measure_name = measure_names[col_idx]
            date_range = date_ranges[col_idx]
            value = row.iloc[col_idx]
            if pd.isna(measure_name) or measure_name == '' or pd.isna(value):
                continue
            
            period_start, period_end = parse_date_range(date_range)
            lower_bound, upper_bound, original_text = parse_range_value(value)
            
            result = {
                'performance_year': performance_year,
                'part_type': 'Part_C',
                'org_type': 'MA-PD',
                'measure_code': str(measure_name).split(':')[0].strip(),
                'measure_name': str(measure_name).split(':')[-1].strip(),
                'measure_period_start': period_start,
                'measure_period_end': period_end,
                'star': star,
                'lower_bound': lower_bound,
                'upper_bound': upper_bound,
                'original_text': original_text
            }
            
            results.append(result)
    
    return pd.DataFrame(results)

def transform_star_ratings(filepath, encoding='utf-8-sig'):
    """Universal transformer that detects format from filename and transforms accordingly
    
    Args:
        filepath: Path to the CSV file
        encoding: File encoding (default: 'utf-8-sig' which handles UTF-8 with BOM)
    """
    # Parse filename for metadata
    performance_year, part_type, generation_date = parse_filename(filepath)
    
    print(f"Filename analysis:")
    print(f"  Measurement Year: {performance_year}")
    print(f"  Part Type: {part_type}")
    print(f"  File Generation Date: {generation_date}")
    
    # Try to read with specified encoding, with fallbacks
    df = None
    encodings_to_try = [encoding, 'utf-8-sig', 'utf-8', 'latin-1', 'iso-8859-1']
    
    for enc in encodings_to_try:
        try:
            df = pd.read_csv(filepath, encoding=enc, dtype=str)
            print(f"  Successfully read with encoding: {enc}")
            break
        except (UnicodeDecodeError, LookupError):
            continue
    
    if df is None:
        raise ValueError(f"Could not read file with any of the attempted encodings: {encodings_to_try}")
    
    # Transform based on part type detected from filename
    output = None
    if part_type == 'Part_D':
        output = transform_star_ratings_part_d(df, performance_year)
    elif part_type == 'Part_C':
        output = transform_star_ratings_part_c(df, performance_year)
    else:
        raise ValueError(f"Unknown format detected in {filepath}")

    output['mimi_src_file_date'] = generation_date
    output['mimi_src_file_name'] = filepath.name
    output['mimi_dlt_load_date'] = datetime.today().date()
    return output

# COMMAND ----------

df_lst = []
for filepath in Path(volumepath).rglob("*Cut Points*.csv"):
    df = transform_star_ratings(filepath)
    df_lst.append(df)
for filepath in Path(volumepath).rglob("*Cutpoints*.csv"):
    df = transform_star_ratings(filepath)
    df_lst.append(df)
df_combined = pd.concat(df_lst)

# COMMAND ----------

spark.createDataFrame(df_combined).write.mode('overwrite').saveAsTable('mimi_ws_1.partcd.star_ratings_cutpoints')

# COMMAND ----------

# MAGIC %md
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.star_ratings_cutpoints IS '# Star Rating Cut Points from the [Part C & D Performance Data (Star Rating) files](https://www.cms.gov/medicare/health-drug-plans/part-c-d-performance-data) | resolution: measure, interval: yearly'

# COMMAND ----------


