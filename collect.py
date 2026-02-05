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

import argparse
import asyncio
import json
import os
import random
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import aiohttp


HN_BASE = "https://hacker-news.firebaseio.com/v0"

# SQLLite Schema

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS stories (
  story_id        INTEGER PRIMARY KEY,
  created_ts      INTEGER NOT NULL,        -- unix seconds
  created_iso     TEXT    NOT NULL,
  author          TEXT,
  title           TEXT,
  url             TEXT,
  domain          TEXT,
  type            TEXT,
  is_dead         INTEGER DEFAULT 0,
  is_deleted      INTEGER DEFAULT 0,
  first_seen_ts   INTEGER NOT NULL,        -- unix seconds (ingestion time)
  raw_json        TEXT                      -- original JSON (optional)
);

CREATE TABLE IF NOT EXISTS snapshots (
  story_id        INTEGER NOT NULL,
  snapshot_ts     INTEGER NOT NULL,        -- unix seconds (when we fetched)
  snapshot_iso    TEXT    NOT NULL,
  score           INTEGER,
  descendants     INTEGER,
  kids_count      INTEGER,
  is_dead         INTEGER DEFAULT 0,
  is_deleted      INTEGER DEFAULT 0,
  PRIMARY KEY (story_id, snapshot_ts)
);

-- NEW: targeted snapshot schedule (must-capture times)
CREATE TABLE IF NOT EXISTS snapshot_targets (
  story_id        INTEGER NOT NULL,
  target_ts       INTEGER NOT NULL,        -- unix seconds (desired capture time)
  kind            TEXT    NOT NULL,        -- 'obs_30m' or 'label_24h' (or more if you add)
  created_ts      INTEGER NOT NULL,        -- story created_ts (for convenience)
  enqueued_ts     INTEGER NOT NULL,        -- when we scheduled it
  done_ts         INTEGER,                 -- when we actually captured it
  PRIMARY KEY (story_id, target_ts, kind)
);

CREATE INDEX IF NOT EXISTS idx_stories_created_ts ON stories(created_ts);
CREATE INDEX IF NOT EXISTS idx_snapshots_story_id ON snapshots(story_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_snapshot_ts ON snapshots(snapshot_ts);

CREATE INDEX IF NOT EXISTS idx_targets_due ON snapshot_targets(done_ts, target_ts);
CREATE INDEX IF NOT EXISTS idx_targets_story ON snapshot_targets(story_id);
"""


# helper funcs for collection post fetch

def iso_utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

def extract_domain(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    # naive domain extractor - handles http(s)://domain/...
    try:
        u = url.split("://", 1)[-1]
        domain = u.split("/", 1)[0].lower()
        if domain.startswith("www."):
            domain = domain[4:]
        return domain[:255]
    except Exception:
        return None

def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.executescript(SCHEMA_SQL)
    return conn

def upsert_story(conn: sqlite3.Connection, item: Dict[str, Any], now_ts: int, store_raw: bool) -> None:
    story_id = item.get("id")
    created_ts = item.get("time")
    if story_id is None or created_ts is None:
        return

    story = (
        int(story_id),
        int(created_ts),
        iso_utc(int(created_ts)),
        item.get("by"),
        item.get("title"),
        item.get("url"),
        extract_domain(item.get("url")),
        item.get("type"),
        1 if item.get("dead") else 0,
        1 if item.get("deleted") else 0,
        int(now_ts),
        json.dumps(item, ensure_ascii=False) if store_raw else None,
    )

    conn.execute(
        """
        INSERT INTO stories (
          story_id, created_ts, created_iso, author, title, url, domain, type,
          is_dead, is_deleted, first_seen_ts, raw_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(story_id) DO UPDATE SET
          -- keep earliest first_seen_ts
          first_seen_ts = MIN(stories.first_seen_ts, excluded.first_seen_ts),
          -- update fields if they were null before (or changed)
          author = COALESCE(excluded.author, stories.author),
          title  = COALESCE(excluded.title,  stories.title),
          url    = COALESCE(excluded.url,    stories.url),
          domain = COALESCE(excluded.domain, stories.domain),
          type   = COALESCE(excluded.type,   stories.type),
          is_dead    = excluded.is_dead,
          is_deleted = excluded.is_deleted,
          -- keep raw_json latest if enabled
          raw_json = COALESCE(excluded.raw_json, stories.raw_json)
        """,
        story,
    )


def insert_snapshot(conn: sqlite3.Connection, story_id: int, item: Dict[str, Any], snapshot_ts: int) -> None:
    score = item.get("score")
    descendants = item.get("descendants")
    kids = item.get("kids") or []
    kids_count = len(kids) if isinstance(kids, list) else None

    row = (
        int(story_id),
        int(snapshot_ts),
        iso_utc(int(snapshot_ts)),
        int(score) if score is not None else None,
        int(descendants) if descendants is not None else None,
        int(kids_count) if kids_count is not None else None,
        1 if item.get("dead") else 0,
        1 if item.get("deleted") else 0,
    )

    conn.execute(
        """
        INSERT OR IGNORE INTO snapshots (
          story_id, snapshot_ts, snapshot_iso, score, descendants, kids_count, is_dead, is_deleted
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        row,
    )


def get_recent_story_ids(conn: sqlite3.Connection, min_created_ts: int) -> List[int]:
    rows = conn.execute(
        "SELECT story_id FROM stories WHERE created_ts >= ? AND type = 'story'",
        (int(min_created_ts),),
    ).fetchall()
    return [int(r[0]) for r in rows]


def enqueue_targets(conn: sqlite3.Connection, story_id: int, created_ts: int, now_ts: int) -> None:
    """
    Schedule must-have snapshots:
      - observation at +30m
      - label snapshot at +24h
    """
    targets = [
        (story_id, created_ts + 30 * 60, "obs_30m"),
        (story_id, created_ts + 24 * 60 * 60, "label_24h"),
    ]
    conn.executemany(
        """
        INSERT OR IGNORE INTO snapshot_targets (story_id, target_ts, kind, created_ts, enqueued_ts, done_ts)
        VALUES (?, ?, ?, ?, ?, NULL)
        """,
        [(sid, tts, kind, created_ts, now_ts) for (sid, tts, kind) in targets],
    )


def get_due_target_story_ids(conn: sqlite3.Connection, now_ts: int, grace_seconds: int, limit: int) -> List[int]:
    """
    Return story_ids that have at least one target due within [target_ts, target_ts + grace_seconds]
    and not yet done. We prioritize the earliest target_ts first.
    """
    rows = conn.execute(
        """
        SELECT story_id
        FROM snapshot_targets
        WHERE done_ts IS NULL
          AND target_ts <= ?
          AND target_ts >= ?  -- optional lower bound (avoid very old missed targets)
        ORDER BY target_ts ASC
        LIMIT ?
        """,
        (int(now_ts), int(now_ts - grace_seconds), int(limit)),
    ).fetchall()
    return [int(r[0]) for r in rows]


def mark_targets_done(conn: sqlite3.Connection, story_id: int, now_ts: int, grace_seconds: int) -> None:
    """
    Mark any due targets for this story as done if they are within the grace window.
    """
    conn.execute(
        """
        UPDATE snapshot_targets
        SET done_ts = ?
        WHERE story_id = ?
          AND done_ts IS NULL
          AND target_ts <= ?
          AND target_ts >= ?
        """,
        (int(now_ts), int(story_id), int(now_ts), int(now_ts - grace_seconds)),
    )


# -----------------------
# HTTP fetching utilities
# -----------------------

@dataclass
class FetchConfig:
    concurrency: int = 30
    timeout_s: int = 15
    max_retries: int = 4


async def fetch_json(session: aiohttp.ClientSession, url: str, cfg: FetchConfig) -> Optional[Dict[str, Any]]:
    for attempt in range(cfg.max_retries):
        try:
            async with session.get(url, timeout=cfg.timeout_s) as resp:
                if resp.status == 200:
                    return await resp.json()
                # transient statuses
                if resp.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep((2 ** attempt) + random.random())
                    continue
                # non-retryable
                text = await resp.text()
                print(f"[warn] {resp.status} for {url}: {text[:200]}", file=sys.stderr)
                return None
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep((2 ** attempt) + random.random())
    print(f"[warn] failed after retries: {url}", file=sys.stderr)
    return None


async def fetch_newstories(session: aiohttp.ClientSession, cfg: FetchConfig) -> List[int]:
    url = f"{HN_BASE}/newstories.json"
    data = await fetch_json(session, url, cfg)
    if not data:
        return []
    return [int(x) for x in data if isinstance(x, int) or (isinstance(x, str) and x.isdigit())]


async def fetch_item(session: aiohttp.ClientSession, item_id: int, cfg: FetchConfig) -> Optional[Dict[str, Any]]:
    url = f"{HN_BASE}/item/{int(item_id)}.json"
    return await fetch_json(session, url, cfg)


# -----------------------
# Main collection loop
# -----------------------

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

    print(f"[info] DB: {db_path}")
    print(f"[info] Poll: every {poll_seconds}s | max_ids_per_poll={max_ids_per_poll} | snapshot_window_hours={snapshot_window_hours}")
    print(f"[info] Concurrency={cfg.concurrency} | retries={cfg.max_retries}")

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

            # 5) background snapshots (optional)
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
            print(
                f"[info] {iso_utc(now_ts)} | stories={n_story} targets_enqueued={n_targets} "
                f"| due_targets={len(due_story_ids)} | snapshots={n_snap} | {elapsed:.1f}s"
            )

            # sleep until next poll
            sleep_for = max(0.0, poll_seconds - elapsed)
            await asyncio.sleep(sleep_for)


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

    args = p.parse_args()

    cfg = FetchConfig(concurrency=args.concurrency, timeout_s=args.timeout, max_retries=args.retries)

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
        print("\n[info] stopped")


if __name__ == "__main__":
    main()
