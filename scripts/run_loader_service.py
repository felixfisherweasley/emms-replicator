#!/usr/bin/env python3

import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.environ['REPLICATOR_LOG_FILE'] = 'loader.log'

from loader.service import run_loader_service
from utils.logging import setup_logging

logger = setup_logging()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run continuous loader service")
    parser.add_argument(
        '--interval-seconds',
        type=int,
        default=None,
        help='Polling interval in seconds (default: from config loader.poll_interval_seconds or 30)'
    )
    args = parser.parse_args()

    logger.info("Starting loader service")
    run_loader_service(poll_interval_seconds=args.interval_seconds)
