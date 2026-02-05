'''
Fetching Utilities
- TODO: Add Description
'''

from __future__ import annotations

import asyncio
import random
import sys
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import aiohttp

HN_BASE = "https://hacker-news.firebaseio.com/v0"

import logging
logger = logging.getLogger(__name__)

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