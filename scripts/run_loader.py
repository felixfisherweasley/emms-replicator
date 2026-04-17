#!/usr/bin/env python3

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.environ['REPLICATOR_LOG_FILE'] = 'loader.log'

from loader.csv_processor import scan_and_load
from utils.logging import setup_logging

logger = setup_logging()

if __name__ == "__main__":
    logger.info("Starting loader")
    scan_and_load()
    logger.info("Loader completed")