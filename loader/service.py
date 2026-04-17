import os
import time
import zipfile

from loader.csv_processor import load_config, scan_and_load
from utils.logging import setup_logging

logger = setup_logging()


def expand_nested_zips(scan_dir):
    """Expand all zip files found in scan_dir recursively, in place."""
    extracted_any = False
    while True:
        zip_paths = []
        for root, _, files in os.walk(scan_dir):
            for file in files:
                if file.lower().endswith('.zip'):
                    zip_paths.append(os.path.join(root, file))

        if not zip_paths:
            break

        for zip_path in zip_paths:
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    zf.extractall(os.path.dirname(zip_path))
                os.remove(zip_path)
                extracted_any = True
                logger.info(f"Expanded zip in loader inbox: {zip_path}")
            except Exception as e:
                logger.error(f"Failed to expand zip {zip_path}: {e}")

    return extracted_any


def run_loader_service(poll_interval_seconds=None):
    config = load_config()
    scan_dir = config['loader']['scan_dir']
    interval = poll_interval_seconds
    if interval is None:
        interval = int(config['loader'].get('poll_interval_seconds', 30))

    os.makedirs(scan_dir, exist_ok=True)
    logger.info(
        f"Starting loader service. scan_dir={scan_dir}, poll_interval_seconds={interval}"
    )

    try:
        while True:
            expand_nested_zips(scan_dir)
            scan_and_load()
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Loader service stopped by user")
