'''
Main Loop
- TODO: Add Description
'''

from typing import Any, Dict, Iterable, List, Optional, Tuple
import random
import sqlite3
import time
import asyncio

from utils.fetching import (
    FetchConfig, 
    fetch_newstories, 
    fetch_item
)
from utils.helpers import (
    init_db, 
    upsert_story, 
    insert_snapshot, 
    enqueue_targets, 
    get_due_target_story_ids, 
    mark_targets_done, 
    get_recent_story_ids, 
    iso_utc
)

import aiohttp

import logging
logger = logging.getLogger(__name__)

async def run_collector(
    db_path: str,
    poll_seconds: int,
    max_ids_per_poll: int,
    snapshot_window_hours: int,
    store_raw_json: bool,
    cfg: FetchConfig,
    target_grace_seconds: int,
    max_target_fetch_per_poll: int,
    enable_background_snapshots: bool,
) -> None:
    conn = init_db(db_path)
    conn.row_factory = sqlite3.Row

    logger.info(f"[info] DB: {db_path}")
    logger.info(f"[info] Poll: every {poll_seconds}s | max_ids_per_poll={max_ids_per_poll} | snapshot_window_hours={snapshot_window_hours}")
    logger.info(f"[info] Concurrency={cfg.concurrency} | retries={cfg.max_retries}")

    connector = aiohttp.TCPConnector(limit=cfg.concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(cfg.concurrency)

        async def guarded_fetch_item(item_id: int) -> Optional[Dict[str, Any]]:
            async with sem:
                return await fetch_item(session, item_id, cfg)

        while True:
            loop_start = time.time()
            now_ts = int(time.time())

            # 1) fetch new story ids
            ids = await fetch_newstories(session, cfg)
            ids = ids[:max_ids_per_poll] if max_ids_per_poll > 0 else ids

            # 2) fetch their items
            tasks = [asyncio.create_task(guarded_fetch_item(i)) for i in ids]
            items = await asyncio.gather(*tasks)

            # 3) write stories + immediate snapshot
            n_story = 0
            n_snap = 0
            n_targets = 0

            with conn:
                for item in items:
                    if not item:
                        continue
                    if item.get("type") != "story":
                        continue
                    story_id = item.get("id")
                    created_ts = item.get("time")
                    if story_id is None or created_ts is None:
                        continue

                    upsert_story(conn, item, now_ts=now_ts, store_raw=store_raw_json)
                    insert_snapshot(conn, int(story_id), item, snapshot_ts=now_ts)

                    # NEW: schedule targets (idempotent)
                    enqueue_targets(conn, int(story_id), int(created_ts), now_ts)
                    n_targets += 1

                    n_story += 1
                    n_snap += 1

            # 4) targeted snapshots that are due (priority)
            due_story_ids = get_due_target_story_ids(
                conn,
                now_ts=now_ts,
                grace_seconds=target_grace_seconds,
                limit=max_target_fetch_per_poll,
            )

            if due_story_ids:
                target_tasks = [asyncio.create_task(guarded_fetch_item(i)) for i in due_story_ids]
                target_items = await asyncio.gather(*target_tasks)

                with conn:
                    for item in target_items:
                        if not item or item.get("type") != "story":
                            continue
                        sid = item.get("id")
                        if sid is None:
                            continue
                        insert_snapshot(conn, int(sid), item, snapshot_ts=now_ts)
                        mark_targets_done(conn, int(sid), now_ts=now_ts, grace_seconds=target_grace_seconds)
                        n_snap += 1

            # 5) background snapshots
            if enable_background_snapshots:
                min_created = now_ts - int(snapshot_window_hours * 3600)
                recent_story_ids = get_recent_story_ids(conn, min_created_ts=min_created)

                if len(recent_story_ids) > 2000:
                    recent_story_ids = random.sample(recent_story_ids, 2000)

                snap_tasks = [asyncio.create_task(guarded_fetch_item(i)) for i in recent_story_ids]
                snap_items = await asyncio.gather(*snap_tasks)

                with conn:
                    for item in snap_items:
                        if not item or item.get("type") != "story":
                            continue
                        story_id = item.get("id")
                        if story_id is None:
                            continue
                        insert_snapshot(conn, int(story_id), item, snapshot_ts=now_ts)
                        n_snap += 1

            elapsed = time.time() - loop_start
            logger.info(
                f"[info] {iso_utc(now_ts)} | stories={n_story} targets_enqueued={n_targets} "
                f"| due_targets={len(due_story_ids)} | snapshots={n_snap} | {elapsed:.1f}s"
            )

            # sleep until next poll
            sleep_for = max(0.0, poll_seconds - elapsed)
            await asyncio.sleep(sleep_for)
