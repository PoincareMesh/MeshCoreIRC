import asyncio
import logging
import re
from datetime import datetime, timezone

from meshcore import MeshCore, EventType

from bridge import Bridge, sanitize_nick

logger = logging.getLogger(__name__)

_MC_MENTION_RE = re.compile(r'@\[([^\]]+)\]')


def _mc_to_irc_mention(text: str, bridge=None) -> str:
    """Convert MeshCore '@[Name]' → IRC '@irc_nick', looking up the sanitized nick via bridge."""
    def _replace(m):
        mc_name = m.group(1)
        irc_nick = bridge.assign_contact_nick(mc_name) if bridge else sanitize_nick(mc_name)
        space = '' if m.end() >= len(text) or text[m.end()].isspace() else ' '
        return f"@{irc_nick}{space}"
    return _MC_MENTION_RE.sub(_replace, text)


class MeshCoreHandler:
    def __init__(self, bridge: Bridge):
        self.bridge = bridge

    async def run(self):
        tty = self.bridge.config['meshcore']['tty']
        baudrate = self.bridge.config['meshcore'].get('baudrate', 115200)

        asyncio.create_task(self._expire_members_loop())

        while True:
            try:
                logger.info("Connecting to MeshCore on %s at %d baud", tty, baudrate)
                mc = await MeshCore.create_serial(tty, baudrate=baudrate, auto_reconnect=False)
                self.bridge.mc = mc

                mc.subscribe(EventType.CONTACT_MSG_RECV, self._on_contact_msg)
                mc.subscribe(EventType.CHANNEL_MSG_RECV, self._on_channel_msg)
                mc.subscribe(EventType.ADVERTISEMENT, self._on_advertisement)
                mc.subscribe(EventType.NEW_CONTACT, self._on_new_contact)
                mc.subscribe(EventType.CONNECTED, self._on_connected)
                mc.subscribe(EventType.DISCONNECTED, self._on_disconnected)

                await mc.ensure_contacts()
                self.bridge.contacts.update(mc.contacts)
                logger.info("Loaded %d contacts (%d total in memory)", len(mc.contacts), len(self.bridge.contacts))
                for contact in self.bridge.contacts.values():
                    name = contact.get('adv_name', '')
                    if name:
                        self.bridge.assign_contact_nick(name)
                revalidated = self.bridge.revalidate_advert_path_nodes()
                added = self.bridge.populate_paths_from_contacts()
                if added:
                    logger.info("Seeded hop cache from out_path for %d contacts", added)
                if revalidated or added:
                    self._save_hops_cache()

                await self._load_channels()
                mc.set_decrypt_channel_logs(True)
                await mc.start_auto_message_fetching()

                si = mc.self_info
                if si:
                    self.bridge.self_info = si
                    logger.info("Connected as: %s [%s]", si.get('name'), si.get('public_key', '?')[:12])
                    new_nick = sanitize_nick(si['name'])
                    self.bridge.rename_irc_clients(new_nick)
                    self.bridge.broadcast_system(
                        f"Connected to MeshCore node: {si.get('name', '?')}  "
                        f"[{si.get('public_key', '?')[:12]}]  "
                        f"{len(self.bridge.contacts)} contacts"
                    )

                self.bridge.resync_irc_clients_to_channels()

                await mc.dispatcher.wait_for_event(EventType.DISCONNECTED, timeout=None)
                logger.warning("MeshCore disconnected — reconnecting in 5s")
                self.bridge.broadcast_system("MeshCore disconnected — reconnecting in 5s")

            except Exception as e:
                logger.error("MeshCore connection error: %s", e, exc_info=True)
                self.bridge.broadcast_system(f"MeshCore error: {e} — reconnecting in 5s")
            finally:
                self.bridge.mc = None
                await asyncio.sleep(5)

    async def _load_channels(self):
        mc = self.bridge.mc
        for idx in range(8):
            try:
                event = await mc.commands.get_channel(idx)
                if not event.is_error():
                    payload = event.payload
                    name = (payload.get('name') or payload.get('channel_name') or '').strip('\x00').strip()
                    if name:
                        self.bridge.channels[idx] = name
                        logger.info("Channel %d: %s", idx, name)
            except Exception as e:
                logger.debug("Channel %d not available: %s", idx, e)

    def _on_connected(self, event):
        logger.info("MeshCore (re)connected")
        self.bridge.broadcast_system("MeshCore (re)connected")

    def _on_disconnected(self, event):
        logger.warning("MeshCore disconnected")

    def _on_contact_msg(self, event):
        payload = event.payload
        pubkey_prefix = payload.get('pubkey_prefix', '')
        text = payload.get('text', '')
        path_len = payload.get('path_len', -1)

        contact = self.bridge.contact_for_pubkey_prefix(pubkey_prefix)
        if not contact and self.bridge.mc:
            # Fall back to device contacts in case bridge.contacts doesn't have it yet
            for pubkey, c in self.bridge.mc.contacts.items():
                if pubkey.lower().startswith(pubkey_prefix.lower()):
                    contact = c
                    self.bridge.contacts[pubkey] = c
                    break

        if contact:
            nick = self.bridge.assign_contact_nick(contact.get('adv_name', 'unknown'))
        else:
            nick = f'_{pubkey_prefix[:8]}' if pubkey_prefix else '_unknown'

        # Send the DM to each connected client targeted at that client's own nick
        for client in list(self.bridge.irc_clients):
            if client.registered:
                client.send(f":{nick}!{pubkey_prefix[:12] or 'anon'}@meshcore PRIVMSG {client.nick} :{_mc_to_irc_mention(text, self.bridge)}")
        logger.info("DM from %s (hops=%d): %s", nick, path_len, text[:60])

    def _on_channel_msg(self, event):
        payload = event.payload
        pubkey_prefix = payload.get('pubkey_prefix', '')
        channel_idx = payload.get('channel_idx', 0)
        text = payload.get('text', '')
        path_len = payload.get('path_len', -1)

        contact = None
        if pubkey_prefix:
            contact = self.bridge.contact_for_pubkey_prefix(pubkey_prefix)
            nick = (self.bridge.assign_contact_nick(contact.get('adv_name', 'unknown'))
                    if contact else f'_{pubkey_prefix[:8]}')
            host = pubkey_prefix[:12]
        else:
            # Channel messages embed the sender as "Name: message" in the text.
            # _split_channel_text returns the original name (may contain emoji etc.) so
            # assign_contact_nick can register it for @mention reverse-lookup.
            raw_name, text = self._split_channel_text(text)
            nick = self.bridge.assign_contact_nick(raw_name) if raw_name != 'mesh' else 'mesh'
            host = 'mesh'

        if self.bridge.is_blocked(nick, host):
            logger.debug("Blocked channel message from %s", nick)
            return

        # Resolve path hashes to node names when decrypt_channels provided a path
        path_hex = payload.get('path', '')
        path_hash_mode = payload.get('path_hash_mode', 0)
        if path_hex and path_hash_mode >= 0 and nick not in ('mesh', 'unknown'):
            hash_chars = (path_hash_mode + 1) * 2
            nodes = []
            for i in range(0, len(path_hex), hash_chars):
                h = path_hex[i:i + hash_chars]
                if not h:
                    continue
                hop = self.bridge.contact_for_pubkey_prefix(h)
                nodes.append(sanitize_nick(hop.get('adv_name', h)) if hop else f'?{h}')
            if nodes:
                self.bridge.channel_msg_path_nodes[nick] = nodes
                if self.bridge.node_cache:
                    if host != 'mesh':
                        self.bridge.node_cache.update_msg_path(host, nodes, path_hash_mode)
                    else:
                        self.bridge.node_cache.update_msg_path_by_nick(nick, nodes, path_hash_mode)

        irc_channel = self.bridge.irc_channel_for_idx(channel_idx)
        self.bridge.update_channel_member(irc_channel, nick, host, path_len=path_len)
        dist_str = ''
        if path_len >= 0:
            src_lat = contact.get('adv_lat', 0.0) if contact else 0.0
            src_lon = contact.get('adv_lon', 0.0) if contact else 0.0
            if not (src_lat or src_lon):
                src_lat, src_lon = self._loc_for_nick(nick)
            si = self.bridge.self_info
            dst_lat = si.get('adv_lat', 0.0) if si else 0.0
            dst_lon = si.get('adv_lon', 0.0) if si else 0.0
            if (src_lat or src_lon) and (dst_lat or dst_lon):
                dist = self.bridge.distance_km(src_lat, src_lon, dst_lat, dst_lon)
                dist_str = f', dist:{dist:.0f}km'
        hops_suffix = f' [hops:{path_len}{dist_str}]' if path_len >= 0 else ''
        self.bridge.broadcast(f":{nick}!{host}@meshcore PRIVMSG {irc_channel} :{_mc_to_irc_mention(text, self.bridge)}{hops_suffix}")

    @staticmethod
    def _split_channel_text(text: str):
        """Split 'SenderName: message' into (original_name, message). Falls back to ('mesh', text).
        Returns the original unmodified name so callers can pass it to assign_contact_nick()
        for proper @mention reverse-lookup — do NOT sanitize here."""
        if ': ' in text:
            name, _, msg = text.partition(': ')
            clean = sanitize_nick(name)
            if clean and clean != 'unknown' and len(name) <= 30:
                return name, msg
        return 'mesh', text

    def _on_advertisement(self, event):
        pubkey = event.payload.get('public_key', '')
        if not pubkey:
            return
        asyncio.create_task(self._handle_advertisement(pubkey))

    async def _handle_advertisement(self, pubkey: str):
        try:
            mc = self.bridge.mc
            if not mc:
                return

            # Always re-fetch from device so updated location data is captured
            fallback = self.bridge.contacts.get(pubkey)
            ev = await mc.commands.get_contact_by_key(bytes.fromhex(pubkey))
            contact = ev.payload if (ev and not ev.is_error()) else fallback
            if contact and contact.get('adv_name'):
                self.bridge.contacts[pubkey] = contact

            if not contact or not contact.get('adv_name'):
                return

            logger.info("Advertisement from %s [%s]", contact.get('adv_name'), pubkey[:12])
            await self._fetch_path_and_announce(pubkey, contact)
        except Exception as e:
            logger.debug("Could not handle advertisement from %s: %s", pubkey[:12], e)

    async def _fetch_path_and_announce(self, pubkey: str, contact: dict):
        mc = self.bridge.mc
        should_announce = True
        if mc:
            try:
                path_ev = await mc.commands.get_advert_path(bytes.fromhex(pubkey))
                if path_ev and not path_ev.is_error():
                    pl = path_ev.payload
                    new_ts = pl.get('timestamp', 0)
                    new_path_len = pl.get('path_len', -1)
                    path_hash_mode = pl.get('path_hash_mode', -1)
                    path_hex = pl.get('path', '')

                    stored_ts = self.bridge.advert_last_ts_by_pubkey.get(pubkey)
                    stored_path_len = self.bridge.advert_path_by_pubkey.get(pubkey, -1)

                    # new_ts == 0 means no timestamp → always treat as new
                    is_new_advert = not new_ts or stored_ts != new_ts
                    is_shorter = (new_path_len >= 0 and
                                  (stored_path_len < 0 or new_path_len < stored_path_len))

                    if is_new_advert or is_shorter:
                        self.bridge.advert_last_ts_by_pubkey[pubkey] = new_ts
                        self.bridge.advert_path_by_pubkey[pubkey] = new_path_len
                        nodes = []
                        if new_path_len > 0 and path_hex and path_hash_mode >= 0:
                            hash_chars = (path_hash_mode + 1) * 2
                            for i in range(0, len(path_hex), hash_chars):
                                h = path_hex[i:i + hash_chars]
                                if not h:
                                    continue
                                hop = self.bridge.contact_for_pubkey_prefix(h)
                                if hop and hop.get('type', 0) != 2:
                                    hop = None  # only repeaters can forward messages
                                nodes.append(
                                    sanitize_nick(hop.get('adv_name', h)) if hop else f'?{h}'
                                )
                        self.bridge.advert_path_nodes_by_pubkey[pubkey] = nodes
                        if self.bridge.node_cache:
                            self.bridge.node_cache.update_path(pubkey, new_path_len, nodes, new_ts,
                                                               path_hash_mode)
                        if is_new_advert:
                            logger.info("Advert path for %s: path_len=%d via %s",
                                        pubkey[:12], new_path_len, nodes or 'direct')
                        else:
                            logger.info("Shorter path for %s: %d (was %d) via %s",
                                        pubkey[:12], new_path_len, stored_path_len, nodes or 'direct')
                            should_announce = False
                        self._save_hops_cache()
                    else:
                        logger.debug("Duplicate advert for %s ts=%d path_len=%d (stored %d), skipped",
                                     pubkey[:12], new_ts, new_path_len, stored_path_len)
                        should_announce = False
                else:
                    reason = path_ev.payload.get('reason', '?') if path_ev else 'no response'
                    logger.warning("get_advert_path failed for %s: %s", pubkey[:12], reason)
            except Exception as e:
                logger.warning("get_advert_path exception for %s: %s", pubkey[:12], e)
        if should_announce:
            self._announce_advert(contact)

    def _announce_advert(self, contact: dict):
        name = contact.get('adv_name', 'unknown')
        pubkey = contact.get('public_key', '')
        if pubkey and name != 'unknown':
            self.bridge.contacts[pubkey] = contact
            if self.bridge.node_cache:
                self.bridge.node_cache.update(contact)
                self.bridge.node_cache.flush()
        lat = contact.get('adv_lat', 0.0)
        lon = contact.get('adv_lon', 0.0)
        # Prefer the incoming advert path length over the stored outgoing path length
        hops = self.bridge.advert_path_by_pubkey.get(pubkey, -1)
        if hops < 0:
            hops = contact.get('out_path_len', -1)
        nick = self.bridge.assign_contact_nick(name)
        via = self.bridge.advert_path_nodes_by_pubkey.get(pubkey, []) if hops >= 0 else []

        parts = [f"Advert: {nick} [{pubkey[:12]}]"]
        if lat or lon:
            parts.append(f"pos={lat:.4f},{lon:.4f}")
        if hops >= 0:
            hops_str = f"hops={hops}"
            if via:
                hops_str += " via " + " → ".join(via)
            parts.append(hops_str)
        else:
            parts.append("flood")

        # Distance: from first node with known position to our location (or last known via node)
        src_lat, src_lon = lat, lon
        if not (src_lat or src_lon):
            for v in via:
                vl, vn = self._loc_for_nick(v)
                if vl or vn:
                    src_lat, src_lon = vl, vn
                    break
        if src_lat or src_lon:
            si = self.bridge.self_info
            dst_lat = si.get('adv_lat', 0.0) if si else 0.0
            dst_lon = si.get('adv_lon', 0.0) if si else 0.0
            if not (dst_lat or dst_lon):
                for v in reversed(via):
                    vl, vn = self._loc_for_nick(v)
                    if vl or vn:
                        dst_lat, dst_lon = vl, vn
                        break
            if dst_lat or dst_lon:
                dist = self.bridge.distance_km(src_lat, src_lon, dst_lat, dst_lon)
                parts.append(f"dist={dist:.1f}km")

        last = contact.get('last_advert', 0)
        if last:
            dt = datetime.fromtimestamp(last, tz=timezone.utc).strftime('%H:%M:%S UTC')
            parts.append(dt)

        self.bridge.broadcast_system('  '.join(parts))

    def _loc_for_nick(self, nick: str) -> tuple:
        """Return (lat, lon) for a via node nick from node_cache, or (0, 0) if unknown."""
        if not self.bridge.node_cache or nick.startswith('?'):
            return 0.0, 0.0
        result = self.bridge.node_cache.get_by_nick(nick)
        if result:
            _, entry = result
            return entry.get('lat', 0.0), entry.get('lon', 0.0)
        return 0.0, 0.0

    def _save_hops_cache(self):
        if self.bridge.node_cache:
            self.bridge.node_cache.flush()

    async def _expire_members_loop(self):
        while True:
            await asyncio.sleep(60)
            self.bridge.expire_channel_members()
            if self.bridge.node_cache:
                self.bridge.node_cache.flush_if_dirty()

    def _on_new_contact(self, event):
        contact = event.payload
        if isinstance(contact, dict):
            pubkey = contact.get('public_key', '')
            if pubkey:
                # Merge into stored contact so fields like out_path_len are preserved
                merged = {**self.bridge.contacts.get(pubkey, {}), **contact}
                self.bridge.contacts[pubkey] = merged
                logger.info("New advert: %s [%s]", merged.get('adv_name'), pubkey[:12])
                asyncio.create_task(self._fetch_path_and_announce(pubkey, merged))
