#!/usr/bin/env python3

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.environ['REPLICATOR_LOG_FILE'] = 'batcher_archive.log'

from batcher.downloader import download_all_zips
from batcher.extractor import extract_all
from utils.logging import setup_logging

logger = setup_logging()


if __name__ == "__main__":
    logger.info("Starting archive batcher")
    download_all_zips(source='archive')
    extract_all(clear_first=False)
    logger.info("Archive batcher completed")
