#!/usr/bin/env python3

from loader.csv_processor import scan_and_load
from utils.logging import setup_logging

logger = setup_logging()

if __name__ == "__main__":
    logger.info("Starting loader")
    scan_and_load()
    logger.info("Loader completed")