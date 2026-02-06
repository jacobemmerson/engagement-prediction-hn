'''
SQL Helpers
- TODO: Add Description
'''

from __future__ import annotations

import logging
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

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

'''
Helper Functions for Processing-
'''

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
