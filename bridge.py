import json
import logging
import re
import math
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

SERVER_NAME = "meshcoreirc"
BOT_NICK = "_MeshCore"
_DEFAULT_MEMBER_TTL = 3600

# Node type byte → display label  (0=none/client, 1=companion, 2=repeater, 3=room, 4=sensor)
NODE_TYPE_LABEL = {0: 'sensor', 1: 'companion', 2: 'repeater', 3: 'room', 4: 'sensor'}

# Filter keyword → matching type values (None = all)
TYPE_FILTERS: dict[str, list[int] | None] = {
    'repeater':  [2],
    'rep':       [2],
    'companion': [1],
    'comp':      [1],
    'sensor':    [0, 4],
    'room':      [3],
    'all':       None,
}


def sanitize_nick(name: str) -> str:
    nick = re.sub(r'[^a-zA-Z0-9_\-\[\]\\`^{}|]', '_', name)
    if not nick:
        nick = 'unknown'
    if nick[0].isdigit():
        nick = '_' + nick
    return nick[:30]


def _sanitize_channel_name(name: str) -> str:
    clean = name.lstrip('#').strip()
    clean = re.sub(r'[^a-zA-Z0-9_\-\[\]\\`^{}|.]', '_', clean)
    return clean


class Bridge:
    def __init__(self, config: dict):
        self.config = config
        self.mc = None
        self.irc_clients: list = []
        self.contacts: dict = {}          # pubkey (64-char hex) -> contact dict
        self.channels: dict = {}          # idx (int) -> channel display name (str)
        self.channel_members: dict = {}   # channel_lower -> {nick: last_seen_float}
        self.repeater_sessions: dict = {} # nick_lower -> contact dict
        self.node_cache = None            # NodeCache, set by main.py
        self.meshcore_map = None          # MeshCoreMapCache, set by main.py
        self.self_info: dict = {}
        self.advert_path_by_pubkey: dict = {}       # pubkey -> best (shortest) incoming path_len
        self.advert_path_nodes_by_pubkey: dict = {} # pubkey -> intermediate node names for best path
        self.advert_last_ts_by_pubkey: dict = {}    # pubkey -> timestamp of last stored advert
        self.min_msg_hops_by_pubkey: dict = {}      # pubkey_prefix (12 chars) -> all-time min msg path_len
        self.channel_msg_path_nodes: dict = {}      # nick -> most-recently-resolved path node names
        # Nick collision registry: ensures two MeshCore names that sanitize to the same
        # IRC nick get distinct nicks (by appending a number suffix).
        self._name_to_nick: dict = {}  # mc_name -> irc_nick
        self._nick_to_name: dict = {}  # irc_nick_lower -> mc_name
        # Block list: channel messages from these users are silently dropped.
        self._blocklist: list = []        # [{'nick': str, 'pubkey_prefix': str}, ...]
        self._blocklist_file: str = ''
        # Repeater password store: saved login passwords, keyed by lowercase nick.
        self._passwords: dict = {}
        self._passwords_file: str = ''

    # ── Block list ────────────────────────────────────────────────────────────

    def load_blocklist(self, file: str):
        self._blocklist_file = file
        try:
            data = json.loads(Path(file).read_text())
            self._blocklist = [e for e in data if isinstance(e, dict) and 'nick' in e]
            if self._blocklist:
                logger.info("Loaded %d block-list entries from %s", len(self._blocklist), file)
        except FileNotFoundError:
            self._blocklist = []
        except Exception as e:
            logger.warning("Could not load blocklist from %s: %s", file, e)
            self._blocklist = []

    def save_blocklist(self):
        if not self._blocklist_file:
            return
        try:
            Path(self._blocklist_file).write_text(json.dumps(self._blocklist, indent=2))
        except Exception as e:
            logger.error("Could not save blocklist to %s: %s", self._blocklist_file, e)

    def is_blocked(self, nick: str, pubkey_prefix: str = '') -> bool:
        nick_lower = nick.lower()
        prefix_lower = pubkey_prefix.lower()
        for entry in self._blocklist:
            ep = entry.get('pubkey_prefix', '').lower()
            if ep and prefix_lower and ep == prefix_lower:
                return True
            if entry['nick'].lower() == nick_lower:
                return True
        return False

    def block_add(self, nick: str, pubkey_prefix: str = ''):
        if not self.is_blocked(nick, pubkey_prefix):
            self._blocklist.append({'nick': nick, 'pubkey_prefix': pubkey_prefix})
            self.save_blocklist()
            return True
        return False

    def block_remove(self, nick: str) -> bool:
        nick_lower = nick.lower()
        before = len(self._blocklist)
        self._blocklist = [e for e in self._blocklist if e['nick'].lower() != nick_lower]
        if len(self._blocklist) < before:
            self.save_blocklist()
            return True
        return False

    def blocklist_entries(self) -> list:
        return list(self._blocklist)

    # ── Repeater password store ───────────────────────────────────────────────

    def load_passwords(self, file: str):
        self._passwords_file = file
        try:
            data = json.loads(Path(file).read_text())
            if isinstance(data, dict):
                self._passwords = {k.lower(): v for k, v in data.items() if isinstance(v, str)}
            if self._passwords:
                logger.info("Loaded %d saved repeater passwords from %s", len(self._passwords), file)
        except FileNotFoundError:
            self._passwords = {}
        except Exception as e:
            logger.warning("Could not load passwords from %s: %s", file, e)
            self._passwords = {}

    def save_passwords(self):
        if not self._passwords_file:
            return
        try:
            Path(self._passwords_file).write_text(json.dumps(self._passwords, indent=2))
        except Exception as e:
            logger.error("Could not save passwords to %s: %s", self._passwords_file, e)

    def password_set(self, nick: str, pwd: str):
        self._passwords[nick.lower()] = pwd
        self.save_passwords()

    def password_get(self, nick: str) -> str:
        return self._passwords.get(nick.lower(), '')

    def password_delete(self, nick: str) -> bool:
        if nick.lower() in self._passwords:
            del self._passwords[nick.lower()]
            self.save_passwords()
            return True
        return False

    def password_list(self) -> list:
        return list(self._passwords.keys())

    def broadcast(self, line: str, exclude=None):
        for client in list(self.irc_clients):
            if client.registered and client is not exclude:
                client.send(line)

    def broadcast_system(self, text: str):
        for client in list(self.irc_clients):
            if client.registered:
                client.send(f":{SERVER_NAME} NOTICE {client.nick} :{text}")

    def rename_irc_clients(self, new_nick: str):
        for client in list(self.irc_clients):
            if client.registered and client.nick != new_nick:
                old_prefix = client.prefix
                client.nick = new_nick
                # Broadcast to ALL clients so everyone's member list stays consistent
                self.broadcast(f":{old_prefix} NICK :{new_nick}")

    def all_irc_channels(self) -> list:
        if self.channels:
            return [self.irc_channel_for_idx(idx) for idx in sorted(self.channels.keys())]
        return [f'#mesh-{i}' for i in range(8)]

    def irc_channel_for_idx(self, idx: int) -> str:
        name = self.channels.get(idx, '')
        if name:
            clean = _sanitize_channel_name(name)
            if clean:
                return f'#{clean}'
        return f'#mesh-{idx}'

    def mc_idx_for_channel(self, irc_channel: str) -> Optional[int]:
        ch = irc_channel.lower()
        for idx in self.channels:
            if self.irc_channel_for_idx(idx).lower() == ch:
                return idx
        if ch.startswith('#mesh-'):
            try:
                return int(ch[6:])
            except ValueError:
                pass
        return None

    def assign_contact_nick(self, mc_name: str) -> str:
        """Return a unique IRC nick for mc_name, disambiguating collisions with a number suffix."""
        if mc_name in self._name_to_nick:
            return self._name_to_nick[mc_name]
        base = sanitize_nick(mc_name)
        nick = base
        suffix = 2
        while nick.lower() in self._nick_to_name and self._nick_to_name[nick.lower()] != mc_name:
            nick = sanitize_nick(base[:28] + str(suffix))
            suffix += 1
        self._name_to_nick[mc_name] = nick
        self._nick_to_name[nick.lower()] = mc_name
        return nick

    def mc_name_for_irc_nick(self, nick: str) -> Optional[str]:
        """Return the original MeshCore name for an IRC nick, or None if unknown."""
        return self._nick_to_name.get(nick.lower())

    def contact_nick(self, contact: dict) -> str:
        return self.assign_contact_nick(contact.get('adv_name', 'unknown'))

    def contact_for_nick(self, nick: str) -> Optional[dict]:
        nick_lower = sanitize_nick(nick).lower()  # normalise spaces→_ before comparing
        for c in self.contacts.values():
            if sanitize_nick(c.get('adv_name', '')).lower() == nick_lower:
                return c
        if self.node_cache:
            result = self.node_cache.get_by_nick(nick)
            if result:
                key, entry = result
                if key.startswith('p:'):
                    return {
                        'public_key': '',
                        '_pubkey_prefix': key[2:],
                        'adv_name': entry['adv_name'],
                        'type': 0,
                        'flags': 0,
                        'out_path': '0' * 128,
                        'out_path_len': -1,
                        'out_path_hash_mode': 0,
                        'last_advert': 0,
                        'adv_lat': 0.0,
                        'adv_lon': 0.0,
                    }
                return {
                    'public_key': key,
                    'adv_name': entry['adv_name'],
                    'type': entry.get('node_type', 0),
                    'flags': 0,
                    'out_path': entry.get('out_path', '0' * 128),
                    'out_path_len': entry.get('out_path_len', -1),
                    'out_path_hash_mode': entry.get('out_path_hash_mode', 0),
                    'last_advert': int(entry.get('last_seen', 0)),
                    'adv_lat': entry.get('lat', 0.0),
                    'adv_lon': entry.get('lon', 0.0),
                }
        return None

    def contact_for_pubkey_prefix(self, prefix: str) -> Optional[dict]:
        prefix_lower = prefix.lower().strip()
        if not prefix_lower:
            return None
        for pubkey, c in self.contacts.items():
            if pubkey.lower().startswith(prefix_lower):
                return c
        return None

    def contacts_map_nodes(self, filter_types=None) -> list:
        saved = self.mc.contacts if self.mc else {}
        nodes = []
        for c in saved.values():
            if filter_types is not None and c.get('type', 0) not in filter_types:
                continue
            lat = c.get('adv_lat', 0)
            lon = c.get('adv_lon', 0)
            if lat or lon:
                nodes.append({'name': self.contact_nick(c), 'lat': lat, 'lon': lon,
                              'type': c.get('type', 0),
                              'ts': c.get('last_advert') or None})
        return nodes

    def discovered_map_nodes(self, filter_types=None) -> list:
        merged: dict = {}
        for pubkey, c in self.contacts.items():
            merged[pubkey] = c
        if self.node_cache:
            for pubkey, entry in self.node_cache.all_items():
                if pubkey not in merged:
                    merged[pubkey] = {
                        'adv_name': entry['adv_name'],
                        'adv_lat': entry.get('lat', 0.0),
                        'adv_lon': entry.get('lon', 0.0),
                        'type': entry.get('node_type', 0),
                        'last_advert': entry.get('last_seen', 0),
                    }
        nodes = []
        for c in merged.values():
            if filter_types is not None and c.get('type', 0) not in filter_types:
                continue
            lat = c.get('adv_lat', 0)
            lon = c.get('adv_lon', 0)
            if lat or lon:
                nodes.append({'name': self.contact_nick(c), 'lat': lat, 'lon': lon,
                              'type': c.get('type', 0),
                              'ts': c.get('last_advert') or None})
        return nodes

    def channel_topic(self, irc_channel: str) -> str:
        idx = self.mc_idx_for_channel(irc_channel)
        if idx is not None:
            name = self.channels.get(idx, '')
            return f"MeshCore Channel {idx}" + (f": {name}" if name else "")
        return ""

    # ── Channel member presence ───────────────────────────────────────────────

    @property
    def member_ttl(self) -> int:
        return self.config.get('irc', {}).get('member_timeout', _DEFAULT_MEMBER_TTL)

    @property
    def voice_ttl(self) -> int:
        return self.config.get('irc', {}).get('voice_timeout', 600)

    @property
    def away_ttl(self) -> int:
        return self.config.get('irc', {}).get('away_timeout', 1800)

    def update_channel_member(self, channel: str, nick: str, host: str = 'mesh',
                              path_len: int = -1):
        # Never track a nick that belongs to a connected IRC client — doing so
        # would cause spurious JOIN/PART/MODE messages that confuse the client.
        if any(c.registered and c.nick == nick for c in self.irc_clients):
            return
        ch = channel.lower()
        members = self.channel_members.setdefault(ch, {})
        now = time.time()
        existing = members.get(nick)

        def _min_path(old: int, new: int) -> int:
            if new < 0:
                return old
            return new if old < 0 else min(old, new)

        if existing is None or now - existing['ts'] >= self.member_ttl:
            # New or fully expired member: JOIN then voice
            members[nick] = {'ts': now, 'host': host, 'voiced': True,
                             'last_path_len': path_len, 'min_path_len': path_len}
            self.broadcast(f":{nick}!{host}@meshcore JOIN :{channel}")
            self.broadcast(f":{SERVER_NAME} MODE {channel} +v {nick}")
        else:
            was_voiced = existing.get('voiced', False)
            was_away = existing.get('away', False)
            members[nick] = {'ts': now, 'host': host, 'voiced': True,
                             'last_path_len': path_len,
                             'min_path_len': _min_path(existing.get('min_path_len', -1), path_len)}
            if was_away:
                self.broadcast(f":{nick}!{host}@meshcore AWAY")
            if not was_voiced:
                # Returning to voice after quiet period
                self.broadcast(f":{SERVER_NAME} MODE {channel} +v {nick}")

        # Update persistent all-time minimum
        if path_len >= 0:
            if host != 'mesh':
                old_min = self.min_msg_hops_by_pubkey.get(host, -1)
                new_min = path_len if old_min < 0 else min(old_min, path_len)
                if new_min != old_min:
                    self.min_msg_hops_by_pubkey[host] = new_min
                    if self.node_cache:
                        self.node_cache.update_channel_node(host, nick, new_min)
            else:
                if self.node_cache:
                    self.node_cache.update_channel_node_by_nick(nick, path_len)

    def active_channel_members(self, channel: str) -> list:
        """Return list of (nick, host, voiced) for members active within member_ttl."""
        ch = channel.lower()
        now = time.time()
        return [
            (nick, m['host'], m.get('voiced', False))
            for nick, m in self.channel_members.get(ch, {}).items()
            if now - m['ts'] < self.member_ttl
        ]

    def channel_member_info(self, nick: str) -> Optional[dict]:
        """Return the most recent channel_members entry for nick across all channels."""
        nick_lower = nick.lower()
        best: Optional[dict] = None
        for ch_members in self.channel_members.values():
            for n, m in ch_members.items():
                if n.lower() == nick_lower:
                    if best is None or m['ts'] > best['ts']:
                        best = m
        return best

    def join_all_clients_to_channel(self, channel: str):
        topic = self.channel_topic(channel)
        ch_lower = channel.lower()
        for client in list(self.irc_clients):
            if client.registered and ch_lower not in client.joined_channels:
                client.joined_channels.add(ch_lower)
                client.send(f":{client.prefix} JOIN :{channel}")
                if topic:
                    client.send(f":{SERVER_NAME} 332 {client.nick} {channel} :{topic}")
                else:
                    client.send(f":{SERVER_NAME} 331 {client.nick} {channel} :No topic is set")
                # Send NAMES so the client's user list is populated immediately.
                irc_nicks = {
                    c.nick for c in self.irc_clients
                    if c.registered and c.nick
                }
                names = [f"@{nick}" for nick in sorted(irc_nicks)]
                for nick, _host, voiced in self.active_channel_members(channel):
                    if nick not in irc_nicks:
                        names.append(f"+{nick}" if voiced else nick)
                client.send(f":{SERVER_NAME} 353 {client.nick} = {channel} :{' '.join(names)}")
                client.send(f":{SERVER_NAME} 366 {client.nick} {channel} :End of /NAMES list")
                client.send(f":{SERVER_NAME} MODE {channel} +o {client.nick}")

    def resync_irc_clients_to_channels(self):
        """Part clients from stale channels, join them to the current real channel list.

        Called after MeshCore loads its channel table so that clients which
        auto-reconnected before MeshCore was ready end up in the correct channels.
        """
        real = {ch.lower(): ch for ch in self.all_irc_channels()}
        for client in list(self.irc_clients):
            if not client.registered:
                continue
            for ch_lower in list(client.joined_channels):
                if ch_lower not in real:
                    display = self._channel_display_name(ch_lower)
                    client.joined_channels.discard(ch_lower)
                    client.send(f":{client.prefix} PART {display} :Channel list updated")
        for ch in real.values():
            self.join_all_clients_to_channel(ch)

    def expire_channel_members(self):
        now = time.time()
        irc_nicks = {c.nick for c in self.irc_clients if c.registered and c.nick}
        for ch, members in self.channel_members.items():
            channel_name = self._channel_display_name(ch)
            for nick in list(members.keys()):
                if nick in irc_nicks:
                    del members[nick]  # silently remove any stale IRC-nick entry
                    continue
                m = members[nick]
                age = now - m['ts']
                if age >= self.member_ttl:
                    self.broadcast(f":{nick}!{m['host']}@meshcore PART {channel_name} :inactive")
                    del members[nick]
                else:
                    if age >= self.voice_ttl and m.get('voiced', False):
                        members[nick]['voiced'] = False
                        self.broadcast(f":{SERVER_NAME} MODE {channel_name} -v {nick}")
                    if age >= self.away_ttl and not m.get('away', False):
                        members[nick]['away'] = True
                        self.broadcast(f":{nick}!{m['host']}@meshcore AWAY :inactive")

    def _channel_display_name(self, channel_lower: str) -> str:
        for ch in self.all_irc_channels():
            if ch.lower() == channel_lower:
                return ch
        return channel_lower

    # ── Hops cache persistence ────────────────────────────────────────────────

    def hops_data(self) -> dict:
        adverts = {}
        for pubkey, path_len in self.advert_path_by_pubkey.items():
            adverts[pubkey] = {
                'path_len': path_len,
                'path_nodes': self.advert_path_nodes_by_pubkey.get(pubkey, []),
                'ts': self.advert_last_ts_by_pubkey.get(pubkey, 0),
            }
        return {'adverts': adverts, 'msg_hops': self.min_msg_hops_by_pubkey}

    def load_hops_data(self, data: dict):
        for pubkey, v in data.get('adverts', {}).items():
            self.advert_path_by_pubkey[pubkey] = v.get('path_len', -1)
            self.advert_path_nodes_by_pubkey[pubkey] = v.get('path_nodes', [])
            self.advert_last_ts_by_pubkey[pubkey] = v.get('ts', 0)
        self.min_msg_hops_by_pubkey.update(data.get('msg_hops', {}))

    def load_msg_paths_from_cache(self):
        """Restore channel_msg_path_nodes from node_cache entries that have persisted msg paths."""
        if not self.node_cache:
            return
        count = 0
        for _key, entry in self.node_cache.all_entries():
            nodes = entry.get('msg_path_nodes')
            if not nodes:
                continue
            name = entry.get('adv_name', '')
            if not name:
                continue
            nick = sanitize_nick(name)
            if nick not in self.channel_msg_path_nodes:
                self.channel_msg_path_nodes[nick] = list(nodes)
                count += 1
        if count:
            logger.info("Restored msg path nodes for %d nodes from cache", count)

    def revalidate_advert_path_nodes(self) -> bool:
        """Drop any stored hop names that are confirmed non-repeaters.
        Returns True if anything changed (caller should re-save the cache)."""
        changed = False
        for pubkey, nodes in list(self.advert_path_nodes_by_pubkey.items()):
            filtered = []
            for name in nodes:
                if name.startswith('?'):
                    filtered.append(name)  # unresolved hash — keep
                    continue
                contact = self.contact_for_nick(name)
                if contact is None or contact.get('type', 0) == 2:
                    filtered.append(name)  # confirmed repeater or not yet known — keep
                else:
                    changed = True  # confirmed non-repeater — drop
            self.advert_path_nodes_by_pubkey[pubkey] = filtered
        return changed

    def populate_paths_from_contacts(self) -> int:
        """Seed advert_path_* for nodes that have a stored out_path but no hop-cache entry.
        Covers both bridge.contacts (saved device contacts) and node_cache (heard nodes).
        out_path hashes are in self→target order; we reverse them to target→self for storage.
        Returns the number of entries added."""
        count = 0

        def _seed(pubkey: str, path_len: int, path_hex: str, hash_mode: int, ts: int):
            if pubkey in self.advert_path_by_pubkey:
                return
            if path_len < 0:
                return  # flood route
            nodes = []
            if path_len > 0 and path_hex and hash_mode >= 0:
                hash_chars = (hash_mode + 1) * 2
                raw = []
                for i in range(0, path_len * hash_chars, hash_chars):
                    h = path_hex[i:i + hash_chars]
                    if not h:
                        continue
                    hop = self.contact_for_pubkey_prefix(h)
                    if hop and hop.get('type', 0) != 2:
                        hop = None
                    raw.append(sanitize_nick(hop.get('adv_name', h)) if hop else f'?{h}')
                nodes = list(reversed(raw))  # flip to target→self order
            self.advert_path_by_pubkey[pubkey] = path_len
            self.advert_path_nodes_by_pubkey[pubkey] = nodes
            self.advert_last_ts_by_pubkey[pubkey] = ts
            if self.node_cache:
                self.node_cache.update_path(pubkey, path_len, nodes, ts, hash_mode)
            nonlocal count
            count += 1

        for pubkey, contact in self.contacts.items():
            _seed(pubkey,
                  contact.get('out_path_len', -1),
                  contact.get('out_path', ''),
                  contact.get('out_path_hash_mode', -1),
                  contact.get('last_advert', 0))

        if self.node_cache:
            for pubkey, entry in self.node_cache.all_items():
                _seed(pubkey,
                      entry.get('out_path_len', -1),
                      entry.get('out_path', ''),
                      entry.get('out_path_hash_mode', 0),
                      0)  # no firmware timestamp in cache; 0 lets next live advert overwrite

        return count

    # ── Geo helpers ───────────────────────────────────────────────────────────

    def osm_link(self, lat: float, lon: float) -> str:
        return f"https://www.openstreetmap.org/?mlat={lat:.6f}&mlon={lon:.6f}&zoom=15"

    def distance_km(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R = 6371.0
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat / 2) ** 2
             + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
        return R * 2 * math.asin(math.sqrt(a))
