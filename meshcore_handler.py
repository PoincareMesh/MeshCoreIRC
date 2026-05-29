import asyncio
import logging
import re
import time
from collections import OrderedDict
from datetime import datetime, timezone

from meshcore import MeshCore, EventType

import map_uploader
from bridge import Bridge, sanitize_nick

logger = logging.getLogger(__name__)

_MC_MENTION_RE = re.compile(r'@\[([^\]]+)\]')

# Dedup cache: drop exact duplicates (same channel/dm key) within this window.
# A message heard via multiple paths (direct + repeater) or delivered N× by stacked
# BLE reconnect clients carries the same sender_timestamp + text → collapsed to one.
_MSG_DEDUP_TTL = 30.0   # seconds
_MSG_DEDUP_MAX = 1024   # max cache entries (oldest evicted beyond this cap)


def _backlog_ts_prefix(ts: int) -> str:
    # sender_timestamp is untrusted mesh input: out-of-range or negative values
    # make datetime.fromtimestamp raise (OverflowError/OSError/ValueError), which
    # would abort the synchronous drain callback and silently lose the backlog —
    # the failure this milestone exists to prevent. Fall back to the sentinel.
    if ts and ts > 0:
        try:
            return '[%s] ' % datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M UTC')
        except (OverflowError, OSError, ValueError):
            pass
    return '[??:?? UTC] '


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
        self._draining: bool = False
        self._path_fetch_sem = asyncio.Semaphore(2)
        self._drain_count: int = 0
        self._recent_msgs: OrderedDict[tuple, float] = OrderedDict()
        self._ble_client = None  # captured BleakClient; disconnected before each reconnect

    def _is_duplicate_msg(self, key: tuple) -> bool:
        """True if this exact (path-independent) message key was seen within _MSG_DEDUP_TTL.

        Collapses the same logical message received via multiple paths (direct + repeater)
        or via stacked BLE reconnect clients. Drops ONLY exact duplicates within the
        window — distinct messages (different text or timestamp) always pass through.
        """
        now = asyncio.get_event_loop().time()
        last = self._recent_msgs.get(key)
        if last is not None and (now - last) < _MSG_DEDUP_TTL:
            return True
        self._recent_msgs[key] = now
        self._recent_msgs.move_to_end(key)
        # prune oldest entries beyond the size cap
        while len(self._recent_msgs) > _MSG_DEDUP_MAX:
            self._recent_msgs.popitem(last=False)
        return False

    async def run(self):
        mc_cfg = self.bridge.config['meshcore']
        ble_address = mc_cfg.get('ble_address', '')
        ble_pin = mc_cfg.get('ble_pin', '') or None
        tty = mc_cfg.get('tty', '')
        baudrate = mc_cfg.get('baudrate', 115200)
        backlog_delay_ms = mc_cfg.get('backlog_delay_ms', 0)
        delay_s = backlog_delay_ms / 1000.0

        asyncio.create_task(self._expire_members_loop())

        while True:
            self._draining = False
            self._drain_count = 0
            mc = None
            # Tear down the previous BLE client before creating a new one so BlueZ
            # notify registrations don't stack. (Serial path has no such stacking.)
            if self._ble_client is not None:
                try:
                    await self._ble_client.disconnect()
                except Exception as e:
                    logger.warning("Error disconnecting previous BLE client: %s", e)
                self._ble_client = None
            try:
                if ble_address:
                    logger.info("Connecting to MeshCore over BLE at %s", ble_address)
                    mc = await MeshCore.create_ble(ble_address, pin=ble_pin, auto_reconnect=False)
                    # Capture the underlying BleakClient so we can disconnect it explicitly
                    # on the next reconnect regardless of library internal _is_connected state.
                    self._ble_client = getattr(
                        getattr(mc.connection_manager, 'connection', None), 'client', None)
                else:
                    logger.info("Connecting to MeshCore on %s at %d baud", tty, baudrate)
                    mc = await MeshCore.create_serial(tty, baudrate=baudrate, auto_reconnect=False)
                self.bridge.mc = mc

                mc.subscribe(EventType.CONTACT_MSG_RECV, self._on_contact_msg)
                mc.subscribe(EventType.CHANNEL_MSG_RECV, self._on_channel_msg)
                mc.subscribe(EventType.ADVERTISEMENT, self._on_advertisement)
                mc.subscribe(EventType.NEW_CONTACT, self._on_new_contact)
                mc.subscribe(EventType.RX_LOG_DATA, self._on_rx_log)
                mc.subscribe(EventType.CONNECTED, self._on_connected)
                mc.subscribe(EventType.DISCONNECTED, self._on_disconnected)

                # ── Contact resync: before-snapshot ──────────────────────────────────
                # Capture baseline BEFORE ensure_contacts()/bridge.contacts.update()
                # mutates bridge.contacts in place.
                # Mid-run reconnect: bridge.contacts is already populated; use it.
                # Cold start: bridge.contacts is empty; fall back to node_cache full-
                # pubkey entries (keyed by full pubkey, timestamp value 0 — different
                # clock from companion last_advert, so only the pubkey-presence test
                # fires on cold start, preventing a whole-list fetch storm).
                if self.bridge.contacts:
                    _resync_baseline: dict[str, int] = {
                        pk: c.get('last_advert', 0)
                        for pk, c in self.bridge.contacts.items()
                    }
                elif self.bridge.node_cache is not None:
                    _resync_baseline = {
                        pk: 0
                        for pk, _entry in self.bridge.node_cache.all_items()
                    }
                else:
                    _resync_baseline = {}

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

                # ── Contact resync: after-snapshot + diff + dispatch ──────────────────
                # Diff against the before-snapshot taken above.
                # new: pubkey present after but absent from baseline (CONT-01 / D-02)
                # updated: pubkey in baseline AND companion last_advert is newer (ADVT-01 / D-03)
                new_pubkeys: set[str] = set()
                updated_pubkeys: set[str] = set()
                for pk, c in self.bridge.contacts.items():
                    if pk not in _resync_baseline:
                        new_pubkeys.add(pk)
                    elif c.get('last_advert', 0) > _resync_baseline[pk]:
                        updated_pubkeys.add(pk)

                logger.info("resync: %d new, %d updated contact(s)",
                            len(new_pubkeys), len(updated_pubkeys))

                # Dispatch silent path fetches for new and updated contacts.
                # Each task is wrapped in the Phase-1 semaphore (Semaphore(2)) because
                # _fetch_path_and_announce does NOT acquire it itself — only
                # _handle_advertisement does (SAFE-01 / D-07).
                # Contact dict captured synchronously at create_task time (ADVT-02 /
                # Pitfall 2 capture-before-create_task pattern).
                async def _resync_fetch(pk: str, c: dict) -> None:
                    async with self._path_fetch_sem:
                        await self._fetch_path_and_announce(pk, c, silent=True)

                for pk in sorted(new_pubkeys | updated_pubkeys):
                    c = self.bridge.contacts.get(pk)
                    if not c:
                        continue
                    kind = "new" if pk in new_pubkeys else "updated"
                    logger.debug("resync fetch %s (%s)", pk[:12], kind)
                    asyncio.create_task(_resync_fetch(pk, c))

                await self._load_channels()
                mc.set_decrypt_channel_logs(True)

                self._draining = True
                self._drain_count = 0
                logger.debug("Drain started — explicit get_msg loop until NO_MORE_MSGS")
                deadline = asyncio.get_event_loop().time() + 120.0
                while True:
                    res = await mc.commands.get_msg(timeout=10.0)
                    if res is None or res.type in (EventType.NO_MORE_MSGS, EventType.ERROR):
                        break
                    if res.type in (EventType.CONTACT_MSG_RECV, EventType.CHANNEL_MSG_RECV):
                        self._drain_count += 1
                        # Yield to event loop every 20 messages (SYNC-06 / Landmine 9):
                        # keeps IRC PING/PONG alive on large drains even at delay=0.
                        if self._drain_count % 20 == 0:
                            await asyncio.sleep(0)
                        elif delay_s > 0:
                            await asyncio.sleep(delay_s)
                    if asyncio.get_event_loop().time() > deadline:
                        logger.warning("Drain exceeded 120s deadline — proceeding to live fetch (remaining backlog drains reactively)")
                        break
                logger.info("Drained %d buffered message(s) on connect", self._drain_count)
                self._draining = False
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
                if mc is not None:
                    try:
                        await mc.disconnect()
                    except Exception as e:
                        logger.warning("Error disconnecting old MeshCore client on reconnect: %s", e)
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
                        resp_idx = payload.get('channel_idx', idx)
                        if resp_idx != idx:
                            logger.warning(
                                "Channel response index mismatch: requested %d got %d — storing under %d",
                                idx, resp_idx, resp_idx)
                        self.bridge.channels[resp_idx] = name
                        logger.info("Channel %d: %s", resp_idx, name)
            except Exception as e:
                logger.debug("Channel %d not available: %s", idx, e)

    def _on_connected(self, event):
        logger.info("MeshCore (re)connected")
        self.bridge.broadcast_system("MeshCore (re)connected")

    def _on_disconnected(self, event):
        logger.warning("MeshCore disconnected")

    def _on_contact_msg(self, event):
        payload = event.payload
        # Hoist ts and pubkey_prefix so they are available inside the draining block
        ts = payload.get('sender_timestamp', 0)
        pubkey_prefix = payload.get('pubkey_prefix', '')
        if self._draining:
            logger.debug("Drain DM (ts=%d): %s", ts, payload.get('text', '')[:40])
            # HWM dedup (SYNC-03 / SYNC-05) — skip already-delivered DMs on reconnect
            ss = self.bridge.sync_state
            if ss and ts > 0:
                hwm_key = f"hwm:dm:{pubkey_prefix[:12]}"
                if ts <= ss.get_hwm(hwm_key):
                    logger.debug("Dedup: skip backlog DM ts=%d scope=%s", ts, hwm_key)
                    return
        if self._is_duplicate_msg(('dm', pubkey_prefix,
                                   ts,
                                   payload.get('text', ''))):
            logger.debug("Dropping duplicate DM (ts=%d)", ts)
            return
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

        # Apply [HH:MM UTC] prefix for backlog DMs (SYNC-02); advance HWM after prefix
        if self._draining:
            text = _backlog_ts_prefix(ts) + text
            ss = self.bridge.sync_state
            if ss and ts > 0:
                ss.set_hwm(f"hwm:dm:{pubkey_prefix[:12]}", ts)

        # Send the DM to each connected client targeted at that client's own nick
        for client in list(self.bridge.irc_clients):
            if client.registered:
                client.send(f":{nick}!{pubkey_prefix[:12] or 'anon'}@meshcore PRIVMSG {client.nick} :{_mc_to_irc_mention(text, self.bridge)}")
        logger.info("DM from %s (hops=%d): %s", nick, path_len, text[:60])

    def _on_channel_msg(self, event):
        payload = event.payload
        # Hoist ts and channel_idx so they are available inside the draining block
        ts = payload.get('sender_timestamp', 0)
        channel_idx = payload.get('channel_idx', 0)
        if self._draining:
            logger.debug("Drain channel msg (ts=%d): %s", ts, payload.get('text', '')[:40])
            # HWM dedup (SYNC-03 / SYNC-05): per-channel scope; best-effort semantics
            # (messages from the same channel share one HWM — near-simultaneous messages
            # from slower senders near the HWM boundary may be skipped on reconnect).
            ss = self.bridge.sync_state
            if ss and ts > 0:
                hwm_key = f"hwm:ch:{channel_idx}"
                if ts <= ss.get_hwm(hwm_key):
                    logger.debug("Dedup: skip backlog channel msg ts=%d scope=%s", ts, hwm_key)
                    return
        if self._is_duplicate_msg(('ch', channel_idx,
                                   ts,
                                   payload.get('text', ''))):
            logger.debug("Dropping duplicate channel msg (ts=%d)", ts)
            return
        pubkey_prefix = payload.get('pubkey_prefix', '')
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

        # Apply [HH:MM UTC] prefix for backlog messages (SYNC-02).
        # For old-firmware, text is already split by _split_channel_text above, so the
        # prefix is applied to the body only (never to the pre-split "Name: text" compound).
        # HWM is advanced here (delivery is imminent); broadcast cannot raise.
        if self._draining:
            text = _backlog_ts_prefix(ts) + text
            ss = self.bridge.sync_state
            if ss and ts > 0:
                ss.set_hwm(f"hwm:ch:{channel_idx}", ts)

        if self.bridge.is_blocked(nick, host):
            logger.debug("Blocked channel message from %s", nick)
            return

        # Resolve path hashes to node names when decrypt_channels provided a path.
        # Path data from backlog messages is still useful for the map — not gated on draining.
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
        # Presence skip (SYNC-04): no JOIN / MODE +v emitted for backlog senders during drain
        if not self._draining:
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

    def _on_rx_log(self, event):
        """Capture the raw advert packet payload, keyed by the sender's public key.

        The raw bytes are what map.meshcore.io's uploader API needs to publish a
        node — they include the contact's signed lat/lon, name, type and timestamp.
        We can't reconstruct this from parsed fields, so we stash it here whenever
        an ADVERT packet arrives (payload_type == 4) and persist it in node_cache.
        """
        payload = event.payload or {}
        if payload.get('payload_type') != 4:
            return
        adv_key = payload.get('adv_key', '')
        raw_hex = payload.get('payload', '')
        if not adv_key or not raw_hex:
            return
        if self.bridge.node_cache:
            self.bridge.node_cache.update_raw_advert(
                adv_key, raw_hex, payload.get('adv_timestamp', 0))

    def _on_advertisement(self, event):
        pubkey = event.payload.get('public_key', '')
        if not pubkey:
            return
        # Capture draining flag synchronously before create_task — the coroutine may
        # run after _draining is cleared (ADVT-02 / Pitfall 2).
        is_backlog = self._draining
        asyncio.create_task(self._handle_advertisement(pubkey, is_backlog=is_backlog))

    async def _handle_advertisement(self, pubkey: str, is_backlog: bool = False):
        async with self._path_fetch_sem:
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
                # Suppress map upload for backlog adverts (ADVT-02); live adverts autoshare normally
                if not is_backlog:
                    await self._maybe_autoshare(pubkey, contact)
            except Exception as e:
                logger.debug("Could not handle advertisement from %s: %s", pubkey[:12], e)

    async def _maybe_autoshare(self, pubkey: str, contact: dict):
        """If pubkey is in the auto-share-to-map list, upload its latest raw advert.

        Skipped when the advert has no location or we've uploaded the same node
        within MIN_REUPLOAD_INTERVAL seconds (matches the reference uploader).
        """
        if not self.bridge.autoshare_contains(pubkey):
            return
        if not (contact.get('adv_lat') or contact.get('adv_lon')):
            return
        last = self.bridge.last_map_upload_ts.get(pubkey, 0)
        if time.time() - last < map_uploader.MIN_REUPLOAD_INTERVAL:
            logger.debug("Skipping auto-share for %s — uploaded %ds ago",
                         pubkey[:12], int(time.time() - last))
            return
        cache = self.bridge.node_cache
        entry = cache.get_by_pubkey(pubkey) if cache else None
        raw = entry.get('raw_advert', '') if entry else ''
        if not raw:
            logger.debug("Auto-share: no raw advert yet for %s", pubkey[:12])
            return
        name = contact.get('adv_name', pubkey[:12])
        ok, msg = await map_uploader.upload_node(self.bridge, pubkey, raw)
        if ok:
            logger.info("Auto-shared %s [%s] to map.meshcore.io: %s",
                        name, pubkey[:12], msg.strip()[:160])
            self.bridge.broadcast_system(
                f"Auto-shared {name} [{pubkey[:12]}] to map.meshcore.io")
        else:
            logger.warning("Auto-share upload failed for %s [%s]: %s",
                           name, pubkey[:12], msg)

    async def _fetch_path_and_announce(self, pubkey: str, contact: dict, silent: bool = False):
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
        if should_announce and not silent:
            self._announce_advert(contact)

    def _announce_advert(self, contact: dict):
        name = contact.get('adv_name', 'unknown')
        pubkey = contact.get('public_key', '')
        if pubkey and name != 'unknown':
            self.bridge.contacts[pubkey] = contact
            if self.bridge.node_cache:
                self.bridge.node_cache.update(contact)
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
        pass  # periodic flush in main.py handles writes

    async def _expire_members_loop(self):
        while True:
            await asyncio.sleep(60)
            self.bridge.expire_channel_members()

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
