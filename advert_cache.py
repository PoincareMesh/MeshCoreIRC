import json
import logging
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_DEFAULT_MAX_AGE_HOURS = 48


class AdvertCache:
    def __init__(self, path: str, max_age_hours: float = _DEFAULT_MAX_AGE_HOURS):
        self.path = Path(path)
        self.max_age_hours = max_age_hours
        self.data: dict = {}  # pubkey (64-char hex) -> entry dict
        self._load()

    def update(self, contact: dict):
        pubkey = contact.get('public_key', '')
        name = contact.get('adv_name', '')
        if not pubkey or not name or name == 'unknown':
            return
        self.data[pubkey] = {
            'adv_name': name,
            'lat': contact.get('adv_lat', 0.0),
            'lon': contact.get('adv_lon', 0.0),
            'node_type': contact.get('type', 0),
            'out_path_len': contact.get('out_path_len', -1),
            'out_path': contact.get('out_path', '0' * 128),
            'out_path_hash_mode': contact.get('out_path_hash_mode', 0),
            'last_seen': time.time(),
        }
        self._prune_and_save()

    def all_items(self) -> list[tuple[str, dict]]:
        """Return (pubkey, entry) pairs for all non-expired entries."""
        return list(self.data.items())

    def get_by_pubkey(self, pubkey: str) -> Optional[dict]:
        return self.data.get(pubkey)

    def get_by_prefix(self, prefix: str) -> Optional[dict]:
        prefix_lower = prefix.lower()
        for key, entry in self.data.items():
            if key.lower().startswith(prefix_lower):
                return entry
        return None

    def all_with_location(self) -> list:
        return [e for e in self.data.values() if e.get('lat') or e.get('lon')]

    def _prune_and_save(self):
        cutoff = time.time() - self.max_age_hours * 3600
        before = len(self.data)
        self.data = {k: v for k, v in self.data.items() if v['last_seen'] >= cutoff}
        removed = before - len(self.data)
        if removed:
            logger.debug("Pruned %d expired advert cache entries", removed)
        try:
            with open(self.path, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error("Failed to save advert cache: %s", e)

    def _load(self):
        if not self.path.exists():
            return
        try:
            with open(self.path, encoding='utf-8') as f:
                self.data = json.load(f)
            cutoff = time.time() - self.max_age_hours * 3600
            before = len(self.data)
            self.data = {k: v for k, v in self.data.items() if v['last_seen'] >= cutoff}
            logger.info(
                "Loaded advert cache: %d entries (%d expired pruned)",
                len(self.data), before - len(self.data),
            )
        except Exception as e:
            logger.error("Failed to load advert cache: %s", e)
            self.data = {}
