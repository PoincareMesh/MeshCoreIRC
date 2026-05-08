import asyncio
import json
import logging
import math
import time
import urllib.request
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_KEEP = frozenset({'public_key', 'adv_name', 'adv_lat', 'adv_lon', 'type', 'last_advert'})
_DEFAULT_URL = 'https://map.meshcore.io/api/v1/nodes'


def _dist_sq(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Approximate squared distance — fast enough for closest-node selection."""
    dlat = lat2 - lat1
    dlon = (lon2 - lon1) * math.cos(math.radians((lat1 + lat2) / 2))
    return dlat * dlat + dlon * dlon


class MeshCoreMapCache:

    def __init__(self, cache_file: str, refresh_hours: float = 48.0,
                 url: str = _DEFAULT_URL):
        self._file = Path(cache_file)
        self._refresh_secs = refresh_hours * 3600.0
        self._url = url
        self._nodes: list = []
        self._by_name: dict = {}    # lower_name -> node
        self._by_prefix: dict = {}  # (plen, hex_prefix) -> [list of nodes]
        self._fetched_at: float = 0.0
        self._load_disk()

    # ── Disk ────────────────────────────────────────────────────────────────

    def _load_disk(self):
        if not self._file.exists():
            return
        try:
            data = json.loads(self._file.read_bytes())
            self._fetched_at = float(data.get('fetched_at', 0.0))
            self._nodes = data.get('nodes', [])
            self._build_indexes()
            logger.info('MeshCoreMap: loaded %d nodes from %s',
                        len(self._nodes), self._file)
        except Exception as exc:
            logger.warning('MeshCoreMap: cache load failed: %s', exc)

    def _save_disk(self):
        try:
            self._file.write_text(
                json.dumps({'fetched_at': self._fetched_at, 'nodes': self._nodes},
                           ensure_ascii=False),
                encoding='utf-8',
            )
        except Exception as exc:
            logger.error('MeshCoreMap: cache save failed: %s', exc)

    def _build_indexes(self):
        by_name: dict = {}
        by_prefix: dict = {}
        for n in self._nodes:
            name = (n.get('adv_name') or '').strip().lower()
            if name:
                by_name[name] = n
            pk = (n.get('public_key') or '').lower()
            for plen in (2, 4, 6, 8, 12, 24, 64):
                if len(pk) >= plen:
                    key = (plen, pk[:plen])
                    by_prefix.setdefault(key, []).append(n)
        self._by_name = by_name
        self._by_prefix = by_prefix

    # ── Lookups ─────────────────────────────────────────────────────────────

    def lookup_by_name(self, name: str) -> Optional[dict]:
        return self._by_name.get((name or '').strip().lower())

    def _candidates(self, prefix: str) -> list:
        p = (prefix or '').lower()
        return self._by_prefix.get((len(p), p), [])

    def lookup_by_prefix_unique(self, prefix: str) -> Optional[dict]:
        """Return a node only when the prefix maps to exactly one entry."""
        cands = self._candidates(prefix)
        return cands[0] if len(cands) == 1 else None

    def lookup_by_prefix_closest(self, prefix: str,
                                  ref_lat: float, ref_lon: float) -> Optional[dict]:
        """Return the candidate whose location is closest to (ref_lat, ref_lon).
        Falls back to the unique match if only one exists."""
        cands = [n for n in self._candidates(prefix)
                 if n.get('adv_lat') or n.get('adv_lon')]
        if not cands:
            return None
        if len(cands) == 1:
            return cands[0]
        return min(cands,
                   key=lambda n: _dist_sq(ref_lat, ref_lon,
                                          n['adv_lat'], n['adv_lon']))

    # ── Refresh ─────────────────────────────────────────────────────────────

    @property
    def needs_refresh(self) -> bool:
        return time.time() - self._fetched_at >= self._refresh_secs

    def _do_fetch(self) -> bytes:
        with urllib.request.urlopen(self._url, timeout=30) as r:
            return r.read()

    async def refresh_if_needed(self):
        if not self.needs_refresh:
            return
        logger.info('MeshCoreMap: refreshing from %s', self._url)
        try:
            loop = asyncio.get_running_loop()
            raw = await loop.run_in_executor(None, self._do_fetch)
            nodes = json.loads(raw)
            self._nodes = [
                {k: n[k] for k in _KEEP if k in n}
                for n in nodes
                if n.get('adv_lat') or n.get('adv_lon')
            ]
            self._fetched_at = time.time()
            self._build_indexes()
            self._save_disk()
            logger.info('MeshCoreMap: cached %d nodes to %s',
                        len(self._nodes), self._file)
        except Exception as exc:
            logger.error('MeshCoreMap: refresh failed: %s', exc)


async def run_refresh_loop(cache: MeshCoreMapCache):
    """Background task: keep the cache fresh."""
    while True:
        await cache.refresh_if_needed()
        remaining = max(60.0, cache._refresh_secs - (time.time() - cache._fetched_at))
        await asyncio.sleep(remaining)
