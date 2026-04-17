import zipfile
import os
import shutil
import yaml
from urllib.parse import unquote
from utils.logging import setup_logging

logger = setup_logging()

def load_config():
    with open('config/config.yaml', 'r') as file:
        return yaml.safe_load(file)

def clear_directory(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
        logger.info(f"Cleared directory {directory}")
    os.makedirs(directory, exist_ok=True)

def extract_zip(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    logger.info(f"Extracted {zip_path} to {extract_to}")

def extract_nested_zips(directory):
    # Keep extracting recursively until no nested zip files remain.
    while True:
        nested_zip_paths = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.lower().endswith('.zip'):
                    nested_zip_paths.append(os.path.join(root, file))

        if not nested_zip_paths:
            break

        for zip_path in nested_zip_paths:
            extract_zip(zip_path, os.path.dirname(zip_path))
            os.remove(zip_path)  # remove after extract

def extract_all(clear_first=False):
    config = load_config()
    download_dir = config['batcher']['download_dir']
    extract_dir = config['batcher']['extract_dir']
    start_year = config['batcher']['start_year']
    end_year = config['batcher']['end_year']
    months = config['batcher'].get('months', list(range(1, 13)))
    
    if clear_first:
        clear_directory(extract_dir)
    else:
        os.makedirs(extract_dir, exist_ok=True)
    
    for root, dirs, files in os.walk(download_dir):
        for file in files:
            if not file.lower().endswith('.zip'):
                continue

            # Decode URL-encoded filename for checking (2026 data uses %23 for #)
            decoded_file = unquote(file)
            filename = decoded_file.upper()
            file_year = None
            file_month = None
            try:
                # Find the date portion (8 digits for YYYYMMDD)
                for i, char in enumerate(filename):
                    if char.isdigit() and i + 7 < len(filename):
                        potential_date = filename[i:i+8]
                        if potential_date.isdigit():
                            file_year = int(potential_date[:4])
                            file_month = int(potential_date[4:6])
                            break
            except Exception as e:
                logger.warning(f"Could not extract year/month from {filename}: {e}")
            
            # Check if file is in configured range
            if file_year and file_month:
                if not (start_year <= file_year <= end_year):
                    logger.info(f"Skipping extraction of {file} - year {file_year} not in range {start_year}-{end_year}")
                    continue
                if file_month not in months:
                    logger.info(f"Skipping extraction of {file} - month {file_month} not in configured months")
                    continue

            zip_path = os.path.join(root, file)
            extract_zip(zip_path, extract_dir)
            os.remove(zip_path)  # remove source zip after extraction to avoid reprocessing
    
    extract_nested_zips(extract_dir)