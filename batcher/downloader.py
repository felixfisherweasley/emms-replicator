import requests
import os
import yaml
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import unquote, urljoin
from utils.logging import setup_logging
from utils.tracking import get_tracking_dataframe

logger = setup_logging()

def load_config():
    with open('config/config.yaml', 'r') as file:
        return yaml.safe_load(file)


def has_local_month_file(existing_files_upper, table_upper, year, month):
    yyyymm = f"{year}{month:02d}"
    dvd_prefix = f"PUBLIC_DVD_{table_upper}_"
    archive_prefix_encoded = f"PUBLIC_ARCHIVE%23{table_upper}%23"
    archive_prefix_decoded = f"PUBLIC_ARCHIVE#{table_upper}#"

    for file_name in existing_files_upper:
        if not file_name.endswith('.ZIP'):
            continue
        if yyyymm not in file_name:
            continue
        if (
            file_name.startswith(dvd_prefix)
            or file_name.startswith(archive_prefix_encoded)
            or file_name.startswith(archive_prefix_decoded)
        ):
            return True
    return False

def download_zip(url, dest_dir):
    filename = url.split('/')[-1]
    filepath = os.path.join(dest_dir, filename)
    if os.path.exists(filepath):
        logger.info(f"File {filename} already exists, skipping download")
        return filepath
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            with open(filepath, 'wb') as f:
                f.write(response.content)
            logger.info(f"Downloaded {filename}")
            return filepath
        else:
            logger.error(f"Failed to download {url}: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        return None

def scrape_data_archive(tables):
    config = load_config()
    download_dir = config['batcher']['download_dir']
    start_year = config['batcher']['start_year']
    end_year = config['batcher']['end_year']
    months = config['batcher'].get('months', list(range(1, 13)))  # Default all months
    os.makedirs(download_dir, exist_ok=True)
    base_url = config['sources']['data_archive_base']

    try:
        tracking_df = get_tracking_dataframe()
        loaded_lookup = {
            (str(row['table_name']).lower(), int(row['year']), int(row['month']))
            for _, row in tracking_df.iterrows()
        }
    except Exception:
        loaded_lookup = set()

    def already_loaded(table_name, year, month):
        return (table_name.lower(), year, month) in loaded_lookup

    current_year = datetime.now().year
    # Only use configured years, don't auto-limit
    if end_year > current_year:
        logger.warning(f"end_year {end_year} is in the future, limiting to {current_year}")
        end_year = current_year

    for year in range(end_year, start_year - 1, -1):
        for month in sorted(months, reverse=True):
            # Fast pre-check: skip HTTP scraping when all tables are already loaded
            # or their source ZIPs are already present locally.
            table_status = []
            try:
                existing_files_upper = {
                    name.upper()
                    for name in os.listdir(download_dir)
                    if os.path.isfile(os.path.join(download_dir, name))
                }
            except Exception:
                existing_files_upper = set()

            for table in tables:
                table_upper = table.upper()
                loaded = already_loaded(table, year, month)
                local_zip_exists = has_local_month_file(existing_files_upper, table_upper, year, month)
                table_status.append((table, loaded, local_zip_exists))

            if all(loaded or local_zip_exists for _, loaded, local_zip_exists in table_status):
                logger.info(
                    f"Skipping scrape for {year}/{month:02d} - all tables already loaded or downloaded"
                )
                continue

            month_str = f"{month:02d}"
            month_url = f"{base_url}{year}/MMSDM_{year}_{month_str}/MMSDM_Historical_Data_SQLLoader/DATA/"
            logger.info(f"Scraping {month_url}")
            try:
                response = requests.get(month_url, timeout=30)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    links = soup.find_all('a', href=True)
                    for link in links:
                        href = link['href']
                        if href.lower().endswith('.zip'):
                            zip_url = urljoin(month_url, href)
                            decoded_href = unquote(href)
                            filename = decoded_href.split('/')[-1].upper()
                            for table in tables:
                                table_upper = table.upper()
                                if f"PUBLIC_DVD_{table_upper}_" in filename or f"PUBLIC_ARCHIVE#{table_upper}#" in filename or f"PUBLIC_ARCHIVE%23{table_upper}%23" in filename:
                                    if already_loaded(table, year, month):
                                        logger.info(f"Skipping {table} {year}/{month:02d} - already loaded")
                                        break
                                    download_zip(zip_url, download_dir)
                                    break
                else:
                    logger.warning(f"Failed to access {month_url}: {response.status_code}")
            except Exception as e:
                logger.error(f"Error scraping {month_url}: {e}")

def download_all_zips():
    config = load_config()
    tables = config['tables']
    scrape_data_archive(tables)