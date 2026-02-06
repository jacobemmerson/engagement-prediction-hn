#!/usr/bin/env python3

"""
Active Hacker News collector:
- Polls /v0/newstories.json
- Fetches item/<id>.json for recent stories (500)
- Stores:
  * stories (static fields)
  * snapshots (dynamic score/comment fields)
"""

from __future__ import annotations

import logging, sys
import argparse
import asyncio
from datetime import datetime
from pathlib import Path
from utils.collection_loop import run_collector
from utils.fetching import FetchConfig

def setup_logging(
        level=logging.INFO,
        log_file=None
):
    handlers = []

    format = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(format)
    handlers.append(console)

    # Optional file handler
    if log_file is not None:
        log_folder = Path("logs") 
        log_folder.mkdir(exist_ok=True)

        file_handler = logging.FileHandler(f"logs/{log_file}")
        file_handler.setFormatter(format)
        handlers.append(file_handler)

    logging.basicConfig(
        level=level,
        handlers=handlers,
        force=True
    )

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="hn.sqlite", help="SQLite DB path")
    p.add_argument("--poll-seconds", type=int, default=600, help="Poll interval (seconds)")
    p.add_argument("--max-ids-per-poll", type=int, default=300, help="Max newstories IDs to fetch each poll (0 = all)")
    p.add_argument("--snapshot-window-hours", type=int, default=6, help="How many recent hours of stories to keep snapshotting")
    p.add_argument("--store-raw-json", action="store_true", help="Store full item JSON in stories.raw_json")
    p.add_argument("--concurrency", type=int, default=30)
    p.add_argument("--timeout", type=int, default=15)
    p.add_argument("--retries", type=int, default=4)
    p.add_argument("--target-grace-seconds", type=int, default=3600,
                help="Grace window for satisfying targets (e.g., 3600 = 1 hour)")
    p.add_argument("--max-target-fetch-per-poll", type=int, default=300,
                help="Limit how many due target story IDs to fetch per poll")
    p.add_argument("--no-background-snapshots", action="store_true",
                help="Disable background snapshotting of recent stories")
    p.add_argument("--log_file", action="store_true", help="Whether to use a logging file.")

    args = p.parse_args()

    cfg = FetchConfig(concurrency=args.concurrency, timeout_s=args.timeout, max_retries=args.retries)
    
    log_file = f"{datetime.now().strftime("%Y%m%d_%H%M%S")}_collection.log" if args.log_file else None
    setup_logging(
        level=logging.INFO, 
        log_file=log_file
    )
    logger = logging.getLogger(__name__)

    try:
        asyncio.run(
            run_collector(
                db_path=args.db,
                poll_seconds=args.poll_seconds,
                max_ids_per_poll=args.max_ids_per_poll,
                snapshot_window_hours=args.snapshot_window_hours,
                store_raw_json=args.store_raw_json,
                cfg=cfg,
                target_grace_seconds=args.target_grace_seconds,
                max_target_fetch_per_poll=args.max_target_fetch_per_poll,
                enable_background_snapshots=(not args.no_background_snapshots),
            )
        )

    except KeyboardInterrupt:
        logger.info("\n[info] stopped")


if __name__ == "__main__":
    main()
