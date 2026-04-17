#!/usr/bin/env python3

import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.logging import setup_logging

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run batcher for a single source")
    parser.add_argument(
        '--source',
        choices=['mmsdm', 'archive', 'current'],
        default='mmsdm',
        help='Batcher source to run (default: mmsdm)'
    )
    args = parser.parse_args()

    os.environ['REPLICATOR_LOG_FILE'] = f"batcher_{args.source}.log"

    from batcher.downloader import download_all_zips
    from batcher.extractor import extract_all

    logger = setup_logging()

    logger.info("Starting batcher")
    download_all_zips(source=args.source)
    extract_all()
    logger.info("Batcher completed")