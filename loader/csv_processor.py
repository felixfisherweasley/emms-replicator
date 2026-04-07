import os
import re
import uuid
import pandas as pd
import yaml
from io import StringIO
from urllib.parse import unquote
from sqlalchemy import text
from database.connection import get_engine
from utils.logging import setup_logging
from utils.tracking import is_loaded, mark_loaded

logger = setup_logging()

def load_config():
    with open('config/config.yaml', 'r') as file:
        return yaml.safe_load(file)


def quote_identifier(identifier):
    return '"' + str(identifier).replace('"', '""') + '"'


def safe_index_name(table_name, columns):
    raw_name = f"idx_{table_name.lower()}_{'_'.join(col.lower() for col in columns)}"
    return re.sub(r'[^a-zA-Z0-9_]+', '_', raw_name)


def apply_table_model_constraints(engine, table_name, config):
    table_model = config.get('table_model', {})
    model = table_model.get(table_name.upper(), {})
    primary_key = model.get('primary_key', [])
    indexes = model.get('indexes', [])

    if not primary_key and not indexes:
        return

    with engine.begin() as conn:
        if primary_key:
            pk_cols_sql = ', '.join(quote_identifier(col) for col in primary_key)
            try:
                conn.execute(
                    text(
                        f"ALTER TABLE {quote_identifier(table_name)} "
                        f"ADD PRIMARY KEY ({pk_cols_sql})"
                    )
                )
                logger.info(f"Applied primary key on {table_name}: {primary_key}")
            except Exception as e:
                logger.warning(f"Could not apply primary key on {table_name}: {e}")

        for index_def in indexes:
            if not isinstance(index_def, dict):
                logger.warning(f"Invalid index config on {table_name}: {index_def}")
                continue

            cols = index_def.get('columns', [])
            if not cols:
                continue

            idx_name = index_def.get('name') or safe_index_name(table_name, cols)
            cols_sql = ', '.join(quote_identifier(col) for col in cols)
            try:
                conn.execute(
                    text(
                        f"CREATE INDEX IF NOT EXISTS {quote_identifier(idx_name)} "
                        f"ON {quote_identifier(table_name)} ({cols_sql})"
                    )
                )
                logger.info(f"Applied index {idx_name} on {table_name}: {cols}")
            except Exception as e:
                logger.warning(f"Could not apply index {idx_name} on {table_name}: {e}")


def deduplicate_dataframe_by_primary_key(df, engine, table_name, primary_key):
    if not primary_key:
        return df

    missing_pk_cols = [col for col in primary_key if col not in df.columns]
    if missing_pk_cols:
        logger.warning(
            f"Primary key columns missing in incoming data for {table_name}: {missing_pk_cols}; skipping PK dedupe"
        )
        return df

    # Remove duplicates inside the incoming batch first.
    before = len(df)
    df = df.drop_duplicates(subset=primary_key)
    dropped_internal = before - len(df)
    if dropped_internal > 0:
        logger.info(f"Dropped {dropped_internal} duplicate rows within incoming batch for {table_name}")

    # Remove rows that already exist in target table by primary key using a staging-table anti-join.
    staging_table = f"__stg_{table_name.lower()}_{uuid.uuid4().hex[:12]}"
    quoted_staging = quote_identifier(staging_table)
    quoted_target = quote_identifier(table_name)
    join_predicate = ' AND '.join(
        f"s.{quote_identifier(col)} = t.{quote_identifier(col)}" for col in primary_key
    )
    null_check_col = quote_identifier(primary_key[0])

    try:
        with engine.begin() as conn:
            df.to_sql(staging_table, conn, if_exists='fail', index=False, chunksize=10000)
            dedupe_sql = f"""
                SELECT s.*
                FROM {quoted_staging} s
                LEFT JOIN {quoted_target} t
                  ON {join_predicate}
                WHERE t.{null_check_col} IS NULL
            """
            deduped = pd.read_sql_query(dedupe_sql, conn)
            conn.execute(text(f"DROP TABLE IF EXISTS {quoted_staging}"))
    except Exception as e:
        logger.warning(f"Could not perform staging-table dedupe for {table_name}: {e}")
        try:
            with engine.begin() as cleanup_conn:
                cleanup_conn.execute(text(f"DROP TABLE IF EXISTS {quoted_staging}"))
        except Exception:
            pass
        return df

    removed_existing = len(df) - len(deduped)
    if removed_existing > 0:
        logger.info(f"Skipped {removed_existing} rows already present in {table_name} by primary key")
    return deduped

def determine_table(filename):
    # Map filename to table name, normalized
    # Handle PUBLIC_DVD_TABLENAME_YYYYMMDD format
    name = os.path.splitext(filename)[0].upper()  # Remove .CSV
    
    # Extract table name from PUBLIC_DVD_TABLENAME_YYYYMMDD format
    if name.startswith('PUBLIC_DVD_'):
        # Remove PUBLIC_DVD_ prefix
        name = name[11:]  # len('PUBLIC_DVD_') = 11
        # Remove trailing date suffix only (e.g., _202407010000), preserving underscores in table name.
        table_part = re.sub(r'_\d{8,12}$', '', name)
    elif 'PUBLIC_ARCHIVE#' in name:
        parts = name.split('#')
        if len(parts) > 1:
            table_part = parts[1]
        else:
            table_part = name
    else:
        table_part = name
    
    # Map to actual table names
    mappings = {
        'DISPATCHPRICE': 'DISPATCHPRICE',
        'DISPATCHLOAD': 'DISPATCHLOAD',
        'DISPATCHREGIONSUM': 'DISPATCHREGIONSUM',
        'TRADINGPRICE': 'TRADINGPRICE',
        'TRADINGREGIONSUM': 'TRADINGREGIONSUM',
        'DUDETAIL': 'DUDETAIL',
        'DUDETAILSUMMARY': 'DUDETAILSUMMARY',
        'DISPATCHINTERCONNECTORRES': 'DISPATCHINTERCONNECTORRES',
        'TRADINGINTERCONNECT': 'TRADINGINTERCONNECT',
        'TRANSMISSIONLOSSFACTOR': 'TRANSMISSIONLOSSFACTOR',
        'ROOFTOP_PV_ACTUAL': 'ROOFTOP_PV_ACTUAL',
        'ROOFTOP_PV_FORECAST': 'ROOFTOP_PV_FORECAST'
    }
    return mappings.get(table_part.upper(), 'OTHER_DATA')


def extract_year_month_from_filename(filename):
    decoded_filename = unquote(filename.upper())
    for i in range(len(decoded_filename) - 7):
        candidate = decoded_filename[i:i+8]
        if candidate.isdigit():
            year = int(candidate[:4])
            month = int(candidate[4:6])
            if 1 <= month <= 12:
                return year, month
    return None, None

def convert_datetime_columns(df):
    """Detect and convert datetime columns to proper datetime type.

    NEM timestamps are in AEST (Australian Eastern Standard Time) with NO daylight savings.
    We keep them as timezone-naive values so the stored time matches the source NEM timestamp.
    """
    # AEMO datetime patterns: EFFECTIVEDATE, SETTLEMENTDATE, AUTHORISEDDATE, LASTCHANGED, CREATETIME, etc.
    datetime_patterns = ['DATE', 'TIME']
    
    for col in df.columns:
        col_upper = col.upper()
        # Check if column name suggests it's a datetime
        is_datetime_col = any(pattern in col_upper for pattern in datetime_patterns)
        
        if is_datetime_col and df[col].dtype != 'datetime64[ns]':
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                logger.info(f"Converted column '{col}' to datetime")
            except Exception as e:
                logger.warning(f"Could not convert '{col}' to datetime: {e}")
    
    return df

def process_csv(filepath):
    try:
        config = load_config()

        # Read the file line by line to handle AEMO format
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
        
        # Find the header line (starts with 'I,')
        header_line = None
        data_lines = []
        for line in lines:
            if line.startswith('I,'):
                header_line = line.strip()
            elif line.startswith('D,'):
                data_lines.append(line.strip())
        
        if not header_line or not data_lines:
            logger.warning(f"No valid header or data in {filepath}")
            return
        
        # Create DataFrame
        from io import StringIO
        header_df = pd.read_csv(StringIO(header_line), header=None, dtype=str)
        full_headers = header_df.iloc[0].tolist()
        
        # Find the first column name that is just an integer (after 'I')
        headers_part = full_headers[1:]  # Skip 'I'
        start_col = 1  # Default to skip 'I' only
        for i, h in enumerate(headers_part):
            if h.strip().isdigit():  # Strip whitespace and check if digit-only
                start_col = i + 2  # Skip 'I' and up to the digit column
                break
        
        # Skip columns up to and including the integer column
        headers = full_headers[start_col:]
        
        data_csv = '\n'.join(data_lines)
        # Read data WITHOUT forcing dtype=str - let pandas infer types
        df = pd.read_csv(StringIO(data_csv), header=None, low_memory=False)
        df = df.iloc[:, start_col:]  # Skip the same columns
        df.columns = headers
        
        logger.info(f"Detected start column index: {start_col}, using headers: {headers[:5]}...")  # Debug log
        
        # Detect and convert datetime columns
        df = convert_datetime_columns(df)
        
        table_name = determine_table(os.path.basename(filepath))
        engine = get_engine()
        
        # Extract year and month from filename (format: PUBLIC_DVD_TABLE_YYYYMMDD[HHMM].CSV or PUBLIC_ARCHIVE#TABLE#FILE01#YYYYMMDD[HHMM].ZIP)
        filename = os.path.basename(filepath).upper()
        # Decode URL-encoded filenames (e.g., %23 -> #)
        decoded_filename = unquote(filename)
        year = None
        month = None
        try:
            # Find the date portion (8 digits for YYYYMMDD)
            for i, char in enumerate(decoded_filename):
                if char.isdigit() and i + 7 < len(decoded_filename):
                    potential_date = decoded_filename[i:i+8]
                    if potential_date.isdigit():
                        year = int(potential_date[:4])
                        month = int(potential_date[4:6])
                        break
        except Exception as e:
            logger.warning(f"Could not extract year/month from {decoded_filename}: {e}")
        
        # Handle schema evolution: check existing table and align columns
        existing_columns = []
        table_exists = False
        try:
            with engine.connect() as conn:
                quoted_name = table_name.replace("'", "''")
                result = conn.execute(text(f"PRAGMA table_info('{quoted_name}')"))
                existing_columns = [row['name'] for row in result.mappings()]
                table_exists = True
        except Exception:
            logger.info(f"Table {table_name} does not exist yet; creating empty table from inferred schema")

        if not table_exists:
            # Create the table first using the inferred pandas dtypes, then append data.
            df.head(0).to_sql(table_name, engine, if_exists='fail', index=False)
            apply_table_model_constraints(engine, table_name, config)
            existing_columns = list(df.columns)
            table_exists = True
            logger.info(f"Created empty table {table_name} with inferred schema")
        
        if existing_columns:
            df_columns = set(df.columns)
            missing_columns = [col for col in existing_columns if col not in df_columns]
            
            if missing_columns:
                df = df.assign(**{col: None for col in missing_columns})
                for col in missing_columns:
                    logger.info(f"Added column '{col}' (NULL) to {table_name} from existing table")
            
            # Reorder DataFrame columns to match table order
            df_cols_to_use = [col for col in existing_columns if col in df.columns]
            df = df[df_cols_to_use]

        # Deduplicate based on configured primary key before insert.
        table_model = config.get('table_model', {})
        model = table_model.get(table_name.upper(), {})
        primary_key = model.get('primary_key', [])
        if table_exists and primary_key:
            df = deduplicate_dataframe_by_primary_key(df, engine, table_name, primary_key)

        if df.empty:
            logger.info(f"No new rows to load for {table_name} after PK dedupe")
            if year and month:
                mark_loaded(table_name.lower(), year, month)
            processed_dir = config['loader']['processed_dir']
            os.makedirs(processed_dir, exist_ok=True)
            processed_path = os.path.join(processed_dir, os.path.basename(filepath))
            os.replace(filepath, processed_path)
            return
        
        # To handle large data, use chunksize
        df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=10000)
        logger.info(f"Loaded {len(df)} rows from {filepath} into {table_name}")
        
        # Mark as loaded in tracking file if year/month were extracted
        if year and month:
            mark_loaded(table_name.lower(), year, month)
        
        # Move to the flat processed folder
        processed_dir = config['loader']['processed_dir']
        os.makedirs(processed_dir, exist_ok=True)
        processed_path = os.path.join(processed_dir, os.path.basename(filepath))
        try:
            os.replace(filepath, processed_path)
        except OSError as e:
            logger.error(f"Failed to move processed file {filepath} to {processed_path}: {e}")
            raise
    except Exception as e:
        logger.error(f"Error processing {filepath}: {e}")

def scan_and_load():
    config = load_config()
    scan_dir = config['loader']['scan_dir']
    start_year = config['batcher']['start_year']
    end_year = config['batcher']['end_year']
    months = config['batcher'].get('months', list(range(1, 13)))
    
    for root, dirs, files in os.walk(scan_dir):
        for file in files:
            if file.lower().endswith('.csv'):
                filepath = os.path.join(root, file)
                
                # Extract year and month from filename to check if in configured range
                filename = file.upper()
                # Decode URL-encoded filenames (e.g., %23 -> #)
                decoded_filename = unquote(filename)
                file_year = None
                file_month = None
                try:
                    # Find the date portion (8 digits for YYYYMMDD)
                    for i, char in enumerate(decoded_filename):
                        if char.isdigit() and i + 7 < len(decoded_filename):
                            potential_date = decoded_filename[i:i+8]
                            if potential_date.isdigit():
                                file_year = int(potential_date[:4])
                                file_month = int(potential_date[4:6])
                                break
                except Exception as e:
                    logger.warning(f"Could not extract year/month from {decoded_filename}: {e}")
                
                # Check if file is in configured range
                if file_year and file_month:
                    if not (start_year <= file_year <= end_year):
                        logger.info(f"Skipping {file} - year {file_year} not in range {start_year}-{end_year}")
                        continue
                    if file_month not in months:
                        logger.info(f"Skipping {file} - month {file_month} not in configured months")
                        continue
                    
                    # Check if this table/year/month combo is already loaded
                    table_name = determine_table(file)
                    if is_loaded(table_name.lower(), file_year, file_month):
                        logger.info(f"Skipping {file} - {table_name} {file_year}/{file_month:02d} already loaded")
                        continue
                
                process_csv(filepath)