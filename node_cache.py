import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)
_DEFAULT_MAX_AGE_HOURS = 336  # 14 days


def _sanitize_nick(name: str) -> str:
    nick = re.sub(r'[^a-zA-Z0-9_\-\[\]\\`^{}|]', '_', name)
    if not nick:
        nick = 'unknown'
    if nick[0].isdigit():
        nick = '_' + nick
    return nick[:30]


class NodeCache:
    """Unified persistent cache for MeshCore nodes.

    Replaces AdvertCache (adverts.json) and hops_store (hops_cache.json).

    Key scheme:
      <64-char-hex>   — full-pubkey entry (from advert)
      p:<12-char-hex> — prefix-only entry (channel-message-only node, no advert received)

    Entry fields (full-pubkey):
      adv_name, lat, lon, node_type, out_path_len, out_path, out_path_hash_mode,
      last_seen, advert_path_len, advert_path_nodes, advert_last_ts, min_msg_hops

    Entry fields (prefix-only):
      adv_name, min_msg_hops, last_seen
    """

    def __init__(self, path: str, max_age_hours: float = _DEFAULT_MAX_AGE_HOURS):
        self.path = Path(path)
        self.max_age_hours = max_age_hours
        self._data: dict = {}
        self._dirty = False
        self._load()

    # ── Write ─────────────────────────────────────────────────────────────────

    def update(self, contact: dict):
        """Update advert fields for a full-pubkey entry; merge any prefix entry."""
        pubkey = contact.get('public_key', '')
        name = contact.get('adv_name', '')
        if not pubkey or not name or name == 'unknown':
            return
        existing = self._data.get(pubkey, {})
        entry = {
            **existing,
            'adv_name': name,
            'lat': contact.get('adv_lat', 0.0),
            'lon': contact.get('adv_lon', 0.0),
            'node_type': contact.get('type', 0),
            'out_path_len': contact.get('out_path_len', -1),
            'out_path': contact.get('out_path', '0' * 128),
            'out_path_hash_mode': contact.get('out_path_hash_mode', 0),
            'last_seen': time.time(),
        }
        # Absorb any prefix-only entry for this pubkey
        prefix_key = f'p:{pubkey[:12].lower()}'
        if prefix_key in self._data:
            p = self._data.pop(prefix_key)
            old = existing.get('min_msg_hops', -1)
            pmin = p.get('min_msg_hops', -1)
            entry['min_msg_hops'] = (pmin if old < 0 else
                                     old if pmin < 0 else
                                     min(old, pmin))
        # Absorb any name-only entry for this node
        nick_key = f'n:{_sanitize_nick(name).lower()}'
        if nick_key in self._data:
            n = self._data.pop(nick_key)
            if not entry.get('msg_path_nodes') and n.get('msg_path_nodes'):
                entry['msg_path_nodes'] = n['msg_path_nodes']
                entry['msg_path_hash_mode'] = n.get('msg_path_hash_mode', 0)
            old_min = entry.get('min_msg_hops', -1)
            n_min = n.get('min_msg_hops', -1)
            if n_min >= 0 and (old_min < 0 or n_min < old_min):
                entry['min_msg_hops'] = n_min
        self._data[pubkey] = entry
        self._dirty = True

    def update_path(self, pubkey: str, path_len: int, path_nodes: list, advert_ts: int,
                    hash_mode: int = -1):
        """Update advert hop-path data for a full-pubkey entry."""
        if pubkey not in self._data:
            self._data[pubkey] = {'last_seen': time.time()}
        e = self._data[pubkey]
        e['advert_path_len'] = path_len
        e['advert_path_nodes'] = list(path_nodes)
        e['advert_last_ts'] = advert_ts
        if hash_mode >= 0:
            e['advert_path_hash_mode'] = hash_mode
        self._dirty = True

    def update_msg_path(self, prefix: str, nodes: list, hash_mode: int):
        """Store msg path nodes for a node identified by 12-char pubkey prefix."""
        prefix_lower = prefix.lower()
        for key in self._data:
            if not key.startswith('p:') and key.lower().startswith(prefix_lower):
                self._data[key]['msg_path_nodes'] = list(nodes)
                self._data[key]['msg_path_hash_mode'] = hash_mode
                self._dirty = True
                return
        pkey = f'p:{prefix_lower}'
        existing = self._data.get(pkey, {})
        self._data[pkey] = {
            **existing,
            'msg_path_nodes': list(nodes),
            'msg_path_hash_mode': hash_mode,
            'last_seen': time.time(),
        }
        self._dirty = True

    def update_msg_path_by_nick(self, nick: str, nodes: list, hash_mode: int):
        """Store msg path nodes for a node identified by its sanitized nick name.
        Used when no pubkey_prefix is available (host='mesh' channel messages).
        Updates an existing entry if found, otherwise creates an n:<nick> entry."""
        result = self.get_by_nick(nick)
        if result:
            key, entry = result
            entry['msg_path_nodes'] = list(nodes)
            entry['msg_path_hash_mode'] = hash_mode
        else:
            nkey = f'n:{nick.lower()}'
            existing = self._data.get(nkey, {})
            self._data[nkey] = {
                **existing,
                'adv_name': nick,
                'msg_path_nodes': list(nodes),
                'msg_path_hash_mode': hash_mode,
                'last_seen': time.time(),
            }
        self._dirty = True

    def update_channel_node_by_nick(self, nick: str, min_hops: int):
        """Record min msg hops for a node identified by nick (no pubkey_prefix available).
        Updates an existing entry if found, otherwise creates an n:<nick> entry."""
        if min_hops < 0:
            return
        result = self.get_by_nick(nick)
        if result:
            key, entry = result
            old = entry.get('min_msg_hops', -1)
            if old < 0 or min_hops < old:
                entry['min_msg_hops'] = min_hops
                self._dirty = True
        else:
            nkey = f'n:{nick.lower()}'
            existing = self._data.get(nkey, {})
            old = existing.get('min_msg_hops', -1)
            if old < 0 or min_hops < old:
                self._data[nkey] = {
                    **existing,
                    'adv_name': nick,
                    'min_msg_hops': min_hops,
                    'last_seen': time.time(),
                }
                self._dirty = True

    def update_channel_node(self, prefix: str, nick: str, min_hops: int):
        """Record min msg hops for a node identified by 12-char pubkey prefix."""
        prefix_lower = prefix.lower()
        # Update full-pubkey entry if we have one
        for key in self._data:
            if not key.startswith('p:') and key.lower().startswith(prefix_lower):
                old = self._data[key].get('min_msg_hops', -1)
                if min_hops >= 0 and (old < 0 or min_hops < old):
                    self._data[key]['min_msg_hops'] = min_hops
                    self._dirty = True
                return
        # No full-pubkey entry — create/update prefix entry
        pkey = f'p:{prefix_lower}'
        existing = self._data.get(pkey, {})
        old = existing.get('min_msg_hops', -1)
        if min_hops >= 0 and (old < 0 or min_hops < old):
            self._data[pkey] = {
                **existing,
                'adv_name': nick,
                'min_msg_hops': min_hops,
                'last_seen': time.time(),
            }
            self._dirty = True
        elif pkey not in self._data:
            self._data[pkey] = {
                'adv_name': nick,
                'min_msg_hops': min_hops if min_hops >= 0 else -1,
                'last_seen': time.time(),
            }
            self._dirty = True

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_by_pubkey(self, pubkey: str) -> Optional[dict]:
        return self._data.get(pubkey)

    def get_by_prefix(self, prefix: str) -> Optional[dict]:
        """Return entry dict for the first full-pubkey entry matching prefix."""
        pl = prefix.lower()
        for key, entry in self._data.items():
            if not key.startswith('p:') and key.lower().startswith(pl):
                return entry
        return None

    def get_prefix_node(self, prefix: str) -> Optional[dict]:
        """Return the p:<prefix> entry for a channel-only node, if present."""
        return self._data.get(f'p:{prefix.lower()}')

    def get_by_nick(self, nick: str) -> Optional[tuple]:
        """Return (key, entry) for first entry (any type) whose adv_name matches nick."""
        target = _sanitize_nick(nick).lower()
        for key, entry in self._data.items():
            if _sanitize_nick(entry.get('adv_name', '')).lower() == target:
                return key, entry
        return None

    def all_items(self) -> list:
        """Return (pubkey, entry) pairs for full-pubkey entries only."""
        return [(k, v) for k, v in self._data.items()
                if not k.startswith('p:') and not k.startswith('n:')]

    def all_entries(self) -> list:
        """Return (key, entry) pairs for all entries including p: and n: entries."""
        return list(self._data.items())

    def all_with_location(self) -> list:
        return [e for k, e in self._data.items()
                if not k.startswith('p:') and not k.startswith('n:')
                and (e.get('lat') or e.get('lon'))]

    def hops_data(self) -> dict:
        """Build dict compatible with Bridge.load_hops_data()."""
        adverts: dict = {}
        msg_hops: dict = {}
        for key, entry in self._data.items():
            if key.startswith('n:'):
                continue  # name-only entries have no hop count or advert path
            if key.startswith('p:'):
                prefix = key[2:]
                if entry.get('min_msg_hops', -1) >= 0:
                    msg_hops[prefix] = entry['min_msg_hops']
            else:
                if entry.get('advert_path_len', -1) >= 0:
                    adverts[key] = {
                        'path_len': entry['advert_path_len'],
                        'path_nodes': entry.get('advert_path_nodes', []),
                        'ts': entry.get('advert_last_ts', 0),
                    }
                if entry.get('min_msg_hops', -1) >= 0:
                    msg_hops[key[:12].lower()] = entry['min_msg_hops']
        return {'adverts': adverts, 'msg_hops': msg_hops}

    # ── Persistence ───────────────────────────────────────────────────────────

    def flush(self):
        cutoff = time.time() - self.max_age_hours * 3600
        before = len(self._data)
        self._data = {k: v for k, v in self._data.items()
                      if v.get('last_seen', 0) >= cutoff}
        pruned = before - len(self._data)
        if pruned:
            logger.debug("Pruned %d expired node cache entries", pruned)
        tmp = str(self.path) + '.tmp'
        try:
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(self._data, f, indent=2, ensure_ascii=False)
            os.replace(tmp, str(self.path))
            self._dirty = False
        except Exception as e:
            logger.error("Failed to save node cache: %s", e)

    def flush_if_dirty(self):
        if self._dirty:
            self.flush()

    def _load(self):
        if not self.path.exists():
            self._try_migrate()
            return
        try:
            with open(self.path, encoding='utf-8') as f:
                self._data = json.load(f)
            cutoff = time.time() - self.max_age_hours * 3600
            before = len(self._data)
            self._data = {k: v for k, v in self._data.items()
                          if v.get('last_seen', 0) >= cutoff}
            logger.info("Loaded node cache: %d entries (%d expired pruned)",
                        len(self._data), before - len(self._data))
        except Exception as e:
            logger.error("Failed to load node cache: %s", e)
            self._data = {}

    def _try_migrate(self):
        """One-time migration from adverts.json + hops_cache.json."""
        old_adverts = self.path.parent / 'adverts.json'
        old_hops = self.path.parent / 'hops_cache.json'
        if not old_adverts.exists() and not old_hops.exists():
            return
        now = time.time()
        if old_adverts.exists():
            try:
                with open(old_adverts, encoding='utf-8') as f:
                    adverts = json.load(f)
                for pubkey, entry in adverts.items():
                    self._data[pubkey] = dict(entry)
                logger.info("Migrated %d entries from adverts.json", len(adverts))
            except Exception as e:
                logger.warning("Could not migrate adverts.json: %s", e)
        if old_hops.exists():
            try:
                with open(old_hops) as f:
                    hops = json.load(f)
                for pubkey, v in hops.get('adverts', {}).items():
                    if pubkey in self._data:
                        self._data[pubkey]['advert_path_len'] = v.get('path_len', -1)
                        self._data[pubkey]['advert_path_nodes'] = v.get('path_nodes', [])
                        self._data[pubkey]['advert_last_ts'] = v.get('ts', 0)
                    else:
                        self._data[pubkey] = {
                            'advert_path_len': v.get('path_len', -1),
                            'advert_path_nodes': v.get('path_nodes', []),
                            'advert_last_ts': v.get('ts', 0),
                            'last_seen': now,
                        }
                for prefix, min_hops in hops.get('msg_hops', {}).items():
                    # Attach to existing full-pubkey entry if possible
                    matched = False
                    for key in self._data:
                        if not key.startswith('p:') and key.lower().startswith(prefix.lower()):
                            self._data[key].setdefault('min_msg_hops', min_hops)
                            matched = True
                            break
                    if not matched:
                        self._data[f'p:{prefix}'] = {
                            'adv_name': '',
                            'min_msg_hops': min_hops,
                            'last_seen': now,
                        }
                logger.info("Migrated hop data from hops_cache.json")
            except Exception as e:
                logger.warning("Could not migrate hops_cache.json: %s", e)
        if self._data:
            self.flush()
            logger.info("Migration complete — nodes.json created")
