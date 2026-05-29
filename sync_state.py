import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def channel_key(idx: int) -> str:
    """Return the HWM key for a channel scope (by channel index)."""
    return f"hwm:ch:{idx}"


def dm_key(prefix: str) -> str:
    """Return the HWM key for a DM scope (by pubkey prefix, first 12 chars)."""
    return f"hwm:dm:{prefix[:12]}"


class SyncState:
    """Persistent storage for per-scope dedup high-water marks.

    Stores the highest sender_timestamp seen per channel index and per DM
    pubkey-prefix so that backlog replay can skip already-delivered messages.

    Writes atomically (tmp + os.replace) on every set_hwm call — no deferred
    flush.  Intentionally separate from NodeCache (which has a 1h deferred
    flush and a 14-day age pruner that are unsafe for per-message scalar state).
    """

    def __init__(self, path: str = 'sync_state.json'):
        self.path = Path(path)
        self._hwm: dict = {}
        self._load()

    # ── Persistence ───────────────────────────────────────────────────────────

    def _save(self):
        tmp = str(self.path) + '.tmp'
        try:
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(self._hwm, f, indent=2, ensure_ascii=False)
            os.replace(tmp, str(self.path))
        except Exception as e:
            logger.error("Failed to save sync state: %s", e)

    def _load(self):
        if not self.path.exists():
            return
        try:
            with open(self.path, encoding='utf-8') as f:
                self._hwm = json.load(f)
            logger.info("Loaded sync state: %d HWM entries", len(self._hwm))
        except Exception as e:
            logger.error("Failed to load sync state: %s", e)
            self._hwm = {}

    # ── HWM API ───────────────────────────────────────────────────────────────

    def get_hwm(self, key: str) -> int:
        """Return the current high-water mark for the given scope key (0 if absent)."""
        return self._hwm.get(key, 0)

    def set_hwm(self, key: str, ts: int):
        """Update the high-water mark for key only if ts is strictly greater.

        Flushes to disk immediately after every accepted update (per-call atomic
        write) so the value survives a process crash between drain messages.
        """
        if ts > self._hwm.get(key, 0):
            self._hwm[key] = ts
            self._save()
