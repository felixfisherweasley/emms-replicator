#!/usr/bin/env python3

import argparse
from batcher.downloader import download_all_zips
from batcher.extractor import extract_all
from loader.csv_processor import scan_and_load
from utils.logging import setup_logging

logger = setup_logging()

def run_batcher(source):
    logger.info("Starting batcher")
    download_all_zips(source=source)
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
    parser.add_argument(
        '--source',
        choices=['mmsdm', 'archive', 'current'],
        default='mmsdm',
        help='Batcher source to run (default: mmsdm)'
    )
    args = parser.parse_args()

    if args.batcher:
        run_batcher(args.source)
    elif args.loader:
        run_loader()
    else:
        # Run default backfill source + loader
        run_batcher(args.source)
        run_loader()

if __name__ == "__main__":
    main()