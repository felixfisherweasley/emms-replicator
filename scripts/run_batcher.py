#!/usr/bin/env python3

from batcher.downloader import download_all_zips
from batcher.extractor import extract_all
from utils.logging import setup_logging

logger = setup_logging()

if __name__ == "__main__":
    logger.info("Starting batcher")
    download_all_zips()
    extract_all()
    logger.info("Batcher completed")