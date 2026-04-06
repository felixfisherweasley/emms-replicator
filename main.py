#!/usr/bin/env python3

import argparse
from batcher.downloader import download_all_zips
from batcher.extractor import extract_all
from loader.csv_processor import scan_and_load
from utils.logging import setup_logging

logger = setup_logging()

def run_batcher():
    logger.info("Starting batcher")
    download_all_zips()
    extract_all()
    logger.info("Batcher completed")

def run_loader():
    logger.info("Starting loader")
    scan_and_load()
    logger.info("Loader completed")

def main():
    parser = argparse.ArgumentParser(description="AEMO Data Replicator")
    parser.add_argument('--batcher', action='store_true', help='Run batcher only')
    parser.add_argument('--loader', action='store_true', help='Run loader only')
    args = parser.parse_args()

    if args.batcher:
        run_batcher()
    elif args.loader:
        run_loader()
    else:
        # Run both
        run_batcher()
        run_loader()

if __name__ == "__main__":
    main()