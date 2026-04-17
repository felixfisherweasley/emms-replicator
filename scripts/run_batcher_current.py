#!/usr/bin/env python3

import argparse
import os
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.environ['REPLICATOR_LOG_FILE'] = 'batcher_current.log'

from batcher.downloader import download_all_zips
from batcher.extractor import extract_all
from batcher.downloader import load_config
from utils.logging import setup_logging

logger = setup_logging()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run current-source batcher poller")
    parser.add_argument(
        '--interval-seconds',
        type=int,
        default=None,
        help='Polling interval in seconds (default: from config batcher.current_poll_interval_seconds or 300)'
    )
    args = parser.parse_args()

    config = load_config()
    interval = args.interval_seconds
    if interval is None:
        interval = int(config['batcher'].get('current_poll_interval_seconds', 300))

    logger.info(f"Starting current batcher poller with interval {interval}s")
    try:
        while True:
            download_all_zips(source='current')
            extract_all(clear_first=False)
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Current batcher poller stopped by user")
