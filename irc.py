import asyncio
import contextlib
import datetime
import logging
import re
import ssl
import time
from pathlib import Path
from datetime import timezone

import urllib.parse

import neighbours_store
from bridge import Bridge, SERVER_NAME, BOT_NICK, sanitize_nick

logger = logging.getLogger(__name__)

CREATED = datetime.datetime.now(timezone.utc).strftime("%a %b %d %Y at %H:%M:%S UTC")

# MeshCore node type byte → label  (0=none/client, 1=companion, 2=repeater, 3=room, 4=sensor)
_NODE_TYPE_LABEL = {0: 'sensor', 1: 'companion', 2: 'repeater', 3: 'room', 4: 'sensor'}

# Filter keyword → matching type values (None = all)
# Contact flags bit definitions (from MeshCore firmware SensorManager.h)
# Bit 0 (0x01) = favourite; remaining bits shifted right by 1 give permission bits:
_FLAG_TELEM_BASE        = 0x02  # base telemetry (battery, stats)
_FLAG_TELEM_LOCATION    = 0x04  # GPS location
_FLAG_TELEM_ENVIRONMENT = 0x08  # environment sensors
_TELEM_PERM_MAP = {
    'data':     _FLAG_TELEM_BASE,
    'location': _FLAG_TELEM_LOCATION,
    'sensors':  _FLAG_TELEM_ENVIRONMENT,
    'all':      _FLAG_TELEM_BASE | _FLAG_TELEM_LOCATION | _FLAG_TELEM_ENVIRONMENT,
}

_TYPE_FILTERS: dict[str, list[int] | None] = {
    'repeater':   [2],
    'rep':        [2],
    'companion':  [1],
    'comp':       [1],
    'sensor':     [0, 4],
    'room':       [3],
    'all':        None,
}


def _fmt_telem_perms(flags: int) -> str:
    parts = []
    if flags & _FLAG_TELEM_BASE:
        parts.append('data')
    if flags & _FLAG_TELEM_LOCATION:
        parts.append('location')
    if flags & _FLAG_TELEM_ENVIRONMENT:
        parts.append('sensors')
    return '+'.join(parts) if parts else ''


def _fmt_lpp(lpp: list, reply_fn):
    """Format a MeshCore LPP telemetry list and send each line via reply_fn."""
    if not lpp:
        reply_fn("  (no data)")
        return
    for item in lpp:
        ch = item.get('channel', '?')
        t = item.get('type', '?')
        v = item.get('value')
        if isinstance(v, dict):
            parts = []
            for k2, v2 in v.items():
                parts.append(f"{k2}={v2:.6g}" if isinstance(v2, float) else f"{k2}={v2}")
            reply_fn(f"  ch{ch} {t}: {', '.join(parts)}")
        else:
            reply_fn(f"  ch{ch} {t}: {v}")


def _fmt_age(secs) -> str:
    if secs is None:
        return '?'
    secs = int(secs)
    if secs < 120:
        return f"{secs}s ago"
    if secs < 7200:
        return f"{secs // 60}m ago"
    if secs < 172800:
        return f"{secs // 3600}h ago"
    return f"{secs // 86400}d ago"



def _irc_to_mc_mention(text: str, bridge=None) -> str:
    """Convert every IRC '@nick' in text → MeshCore '@[OriginalName]'.

    Handles both lead mentions (@nick: msg) and mid-message mentions (hello @nick).
    A separator character immediately following the nick (configurable via
    [irc] mention_separators) is consumed; surrounding text is preserved.
    """
    if bridge:
        if not hasattr(bridge, '_mention_inline_re'):
            seps = bridge.config.get('irc', {}).get('mention_separators', ':')
            if isinstance(seps, list):
                seps = ''.join(seps)
            seps = seps or ':'
            sep_class = '[' + re.escape(seps) + ']'
            bridge._mention_inline_re = re.compile(
                r'@([a-zA-Z0-9_\-\[\]\\`^{}|]+)' + sep_class + r'?')
        pattern = bridge._mention_inline_re
    else:
        pattern = re.compile(r'@([a-zA-Z0-9_\-\[\]\\`^{}|]+)[:;,]?')

    def _replace(m):
        irc_nick = m.group(1)
        mc_name = bridge.mc_name_for_irc_nick(irc_nick) if bridge else None
        return f"@[{mc_name if mc_name else irc_nick}]"

    return pattern.sub(_replace, text)
VERSION = "1.0"


class IRCClient:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, bridge: Bridge):
        self.reader = reader
        self.writer = writer
        self.bridge = bridge
        self.nick: str | None = None
        self.username: str | None = None
        self.realname: str | None = None
        self.registered = False
        self._pass_ok = False
        self._got_nick = False
        self._got_user = False
        self.joined_channels: set = set()
        self.addr = writer.get_extra_info('peername', ('?', 0))
        self._neighbours_pending: dict = {}  # nick_lower → {contact, offset, total, key}
        self._last_neighbours_nick: str = ''

    @property
    def prefix(self) -> str:
        return f"{self.nick}!{self.username or 'user'}@meshcore"

    def send(self, line: str):
        logger.debug("IRC >> %s", line)
        try:
            self.writer.write((line + "\r\n").encode('utf-8', errors='replace'))
        except Exception:
            pass

    def numeric(self, code: str, *args: str):
        target = self.nick or '*'
        self.send(f":{SERVER_NAME} {code} {target} {' '.join(args)}")

    async def handle(self):
        try:
            while True:
                data = await self.reader.readline()
                if not data:
                    break
                line = data.decode('utf-8', errors='replace').rstrip('\r\n')
                if line:
                    logger.debug("IRC << %s: %s", self.addr[0], line)
                    await self.dispatch(line)
        except asyncio.CancelledError:
            pass
        except ConnectionError:
            pass
        except Exception as e:
            logger.error("IRC client error from %s: %s", self.addr, e)
        finally:
            await self._disconnect()

    async def _disconnect(self):
        if self in self.bridge.irc_clients:
            self.bridge.irc_clients.remove(self)
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass
        logger.info("IRC client disconnected: %s", self.addr)

    async def dispatch(self, line: str):
        # Strip optional leading prefix
        if line.startswith(':'):
            parts = line.split(' ', 2)
            cmd = parts[1].upper() if len(parts) > 1 else ''
            rest = parts[2] if len(parts) > 2 else ''
        else:
            parts = line.split(' ', 1)
            cmd = parts[0].upper()
            rest = parts[1] if len(parts) > 1 else ''

        if cmd == 'CAP':
            await self._cmd_cap(rest)
        elif cmd == 'PASS':
            await self._cmd_pass(rest)
        elif cmd == 'NICK':
            await self._cmd_nick(rest)
        elif cmd == 'USER':
            await self._cmd_user(rest)
        elif cmd == 'PING':
            token = rest.lstrip(':').strip()
            self.send(f":{SERVER_NAME} PONG {SERVER_NAME} :{token}")
        elif cmd == 'PONG':
            pass
        elif cmd == 'QUIT':
            reason = rest.lstrip(':') or 'Quit'
            if self.registered:
                # Only send QUIT to clients with a different nick; same-nick clients
                # should not see a QUIT because the session is still represented by them.
                for c in list(self.bridge.irc_clients):
                    if c.registered and c is not self and c.nick != self.nick:
                        c.send(f":{self.prefix} QUIT :{reason}")
            self.writer.close()
        elif not self.registered:
            self.numeric('451', ':You have not registered')
        elif cmd == 'JOIN':
            await self._cmd_join(rest)
        elif cmd == 'PART':
            await self._cmd_part(rest)
        elif cmd == 'PRIVMSG':
            await self._cmd_privmsg(rest)
        elif cmd == 'NOTICE':
            pass
        elif cmd == 'WHOIS':
            await self._cmd_whois(rest)
        elif cmd == 'WHO':
            await self._cmd_who(rest)
        elif cmd == 'NAMES':
            await self._cmd_names(rest)
        elif cmd == 'LIST':
            await self._cmd_list()
        elif cmd == 'MODE':
            await self._cmd_mode(rest)
        elif cmd == 'TOPIC':
            await self._cmd_topic(rest)
        elif cmd == 'MOTD':
            self._send_motd()
        elif cmd == 'LUSERS':
            self.numeric('251', f':There are {len(self.bridge.irc_clients)} users on 1 server')
        elif cmd == 'AWAY':
            self.numeric('305', ':You are no longer marked as being away')
        elif cmd == 'ISON':
            nicks = rest.lstrip(':').split()
            online = [n for n in nicks if any(c.nick == n for c in self.bridge.irc_clients if c.registered)]
            self.numeric('303', f':{" ".join(online)}')
        elif cmd == 'USERHOST':
            nick = rest.strip().split()[0] if rest.strip() else ''
            if nick and self.nick and nick.lower() == self.nick.lower():
                self.numeric('302', f':{nick}=+{self.username}@meshcore')
        else:
            logger.debug("Unhandled IRC command: %s %s", cmd, rest)

    # ── Pre-registration ──────────────────────────────────────────────────────

    async def _cmd_cap(self, rest: str):
        parts = rest.split()
        subcmd = parts[0].upper() if parts else ''
        if subcmd == 'LS':
            self.send(f":{SERVER_NAME} CAP * LS :")
        elif subcmd == 'END':
            pass
        elif subcmd == 'REQ':
            caps = rest.split(':', 1)[1] if ':' in rest else ''
            self.send(f":{SERVER_NAME} CAP * NAK :{caps}")

    async def _cmd_pass(self, rest: str):
        if self.registered:
            self.numeric('462', ':Unauthorized command (already registered)')
            return
        password = rest.lstrip(':').strip()
        if password == self.bridge.config['irc']['password']:
            self._pass_ok = True
        else:
            self.numeric('464', ':Password incorrect')
            self.writer.close()

    async def _cmd_nick(self, rest: str):
        if self.registered:
            return  # server-driven renames only; silently ignore client requests

        nick = rest.strip()
        if not nick:
            self.numeric('431', ':No nickname given')
            return
        self._got_nick = True
        self.nick = nick
        await self._try_register()

    async def _cmd_user(self, rest: str):
        if self.registered:
            self.numeric('462', ':Unauthorized command (already registered)')
            return
        parts = rest.split(' ', 3)
        if len(parts) < 4:
            self.numeric('461', 'USER :Not enough parameters')
            return
        self.username = parts[0]
        self.realname = parts[3].lstrip(':')
        self._got_user = True
        await self._try_register()

    async def _try_register(self):
        if not (self._pass_ok and self._got_nick and self._got_user):
            return
        self.registered = True
        self.bridge.irc_clients.append(self)
        logger.info("IRC client registered: %s from %s", self.nick, self.addr)

        self.numeric('001', f':Welcome to MeshCore IRC Gateway, {self.prefix}')
        self.numeric('002', f':Your host is {SERVER_NAME}, running version {VERSION}')
        self.numeric('003', f':This server was created {CREATED}')
        self.numeric('004', f'{SERVER_NAME} meshcoreirc-{VERSION} o o')
        self.numeric('005', 'CHANTYPES=# CASEMAPPING=rfc1459 CHANMODES=,,, PREFIX=(ov)@+ :are supported by this server')
        self.numeric('251', ':There are 1 users on 1 server')
        self._send_motd()

        si = self.bridge.self_info
        if si and si.get('name'):
            new_nick = sanitize_nick(si['name'])
            if new_nick != self.nick:
                self.send(f":{self.prefix} NICK :{new_nick}")
                self.nick = new_nick

        if self.bridge.channels:
            for ch in self.bridge.all_irc_channels():
                await self._do_join(ch)

    def _send_motd(self):
        self.numeric('375', ':- meshcoreirc Message of the Day -')
        self.numeric('372', ':- MeshCore IRC Gateway')
        self.numeric('372', ':-')
        self.numeric('372', ':- Channels:')
        for ch in self.bridge.all_irc_channels():
            self.numeric('372', f':-   {ch}')
        self.numeric('372', ':- Node advertisements arrive as server notices')
        self.numeric('372', ':-')
        self.numeric('372', f':- Commands (via /msg {BOT_NICK} <cmd>):')
        self.numeric('372', ':-   help                       show this list')
        self.numeric('372', ':-   contacts <all|repeater|companion|sensor|room> [filter]')
        self.numeric('372', ':-   discovered <all|repeater|companion|sensor|room> [filter]')
        self.numeric('372', ':-   nodeinfo                   show our own node details')
        self.numeric('372', ':-   login <name> [pwd]         login to a repeater (uses saved pwd if omitted)')
        self.numeric('372', ':-   logout <name>              logout from a repeater')
        self.numeric('372', ':-   synctime <name>            sync repeater clock to gateway time')
        self.numeric('372', ':-   savepassword <name> <pwd>  save repeater login password')
        self.numeric('372', ':-   deletepassword <name>      remove saved password')
        self.numeric('372', ':-   status <name>              request repeater status')
        self.numeric('372', ':-   neighbours <name>          request repeater neighbours')
        self.numeric('372', ':-   expand [name]              fetch next batch of neighbours')
        self.numeric('372', ':-   get                        show all node settings')
        self.numeric('372', ':-   get/set power|radio|name|coords|autoadd|lockey|multiack|telemetry|af|pathmode [value]')
        self.numeric('372', ':-   get tuning|bat|stats|deviceinfo|customs')
        self.numeric('372', ':-')
        self.numeric('372', ':-   refreshcontacts            refresh contact list from companion')
        self.numeric('372', ':-   zeroadvert                 send self-advertisement (zero-hop)')
        self.numeric('372', ':-   floodadvert                send self-advertisement (flood)')
        self.numeric('372', ':-   listchannels               list all configured channel slots')
        self.numeric('372', ':-   addchannel <name>          join MeshCore channel (auto slot)')
        self.numeric('372', ':-   addchannel <idx> <name>    join MeshCore channel at specific slot')
        self.numeric('372', ':-   deletechannel <name|#ch|idx>  delete a channel from companion')
        self.numeric('372', ':-   addcontact <nick|pubkey>                save discovered contact to companion')
        self.numeric('372', ':-   removecontact <nick|pubkey>             remove contact from companion')
        self.numeric('372', ':-   renamecontact <nick|pubkey> <new name>  rename a saved contact')
        self.numeric('372', ':-   resetpath <nick|pubkey>                 reset path to flood (auto-learn)')
        self.numeric('372', ':-   setpath <nick|pubkey> <hex>[:<mode>]   set fixed path')
        self.numeric('372', ':-')
        self.numeric('372', ':-   block / unblock / blocklist   ignore channel messages from a user')
        self.numeric('372', ':- /whois <nick>  shows full contact details (pubkey, position, ...)')
        self.numeric('376', ':End of /MOTD command')

    # ── Post-registration ─────────────────────────────────────────────────────

    async def _cmd_join(self, rest: str):
        for ch in rest.split(','):
            ch = ch.strip().split(' ')[0]
            if ch:
                await self._do_join(ch)

    async def _do_join(self, channel: str):
        known = [c.lower() for c in self.bridge.all_irc_channels()]
        if channel.lower() not in known:
            self.numeric('403', channel, ':No such channel')
            return
        self.joined_channels.add(channel.lower())
        # Only notify clients with a different nick — broadcasting a JOIN from the same
        # nick that a client already has in the channel confuses its member list.
        for c in list(self.bridge.irc_clients):
            if c.registered and c is not self and c.nick != self.nick:
                c.send(f":{self.prefix} JOIN :{channel}")
        self.send(f":{self.prefix} JOIN :{channel}")
        topic = self.bridge.channel_topic(channel)
        if topic:
            self.numeric('332', channel, f':{topic}')
        else:
            self.numeric('331', channel, ':No topic is set')
        await self._send_names(channel)
        self.send(f":{SERVER_NAME} MODE {channel} +o {self.nick}")

    async def _send_names(self, channel: str):
        ch_lower = channel.lower()
        # Include all registered IRC clients regardless of joined_channels state —
        # they are all the same logical user and should each see themselves in the list.
        irc_nicks = {c.nick for c in self.bridge.irc_clients if c.registered and c.nick}
        irc_nicks.add(self.nick)  # always include self
        names = [f"@{nick}" for nick in sorted(irc_nicks)]
        for nick, _host, voiced in self.bridge.active_channel_members(channel):
            if nick not in irc_nicks:
                names.append(f"+{nick}" if voiced else nick)
        self.numeric('353', f'= {channel}', f':{" ".join(names)}')
        self.numeric('366', channel, ':End of /NAMES list')

    async def _cmd_part(self, rest: str):
        parts = rest.split(' ', 1)
        channel = parts[0].strip()
        reason = parts[1].lstrip(':') if len(parts) > 1 else ''
        self.joined_channels.discard(channel.lower())
        # Only broadcast PART to clients with a different nick; clients sharing the same
        # nick should not see a PART because they are still present in the channel.
        for c in list(self.bridge.irc_clients):
            if c.registered and c is not self and c.nick != self.nick:
                c.send(f":{self.prefix} PART {channel} :{reason}")

    async def _cmd_privmsg(self, rest: str):
        if ' :' not in rest:
            self.numeric('461', 'PRIVMSG :Not enough parameters')
            return
        target, text = rest.split(' :', 1)
        target = target.strip()
        if not text:
            return

        if target.lower() == BOT_NICK.lower():
            await self._handle_bot(text)
            return

        if target.startswith('#'):
            idx = self.bridge.mc_idx_for_channel(target)
            if idx is None:
                self.numeric('403', target, ':No such channel')
                return
            mc_text = _irc_to_mc_mention(text, self.bridge)
            max_len = 200
            byte_len = len(mc_text.encode('utf-8'))
            if byte_len > max_len:
                self.send(f":{BOT_NICK}!bot@meshcore PRIVMSG {target} :Msg too long: {byte_len}/{max_len} (+{byte_len - max_len})")
                return
            self.bridge.broadcast(f":{self.prefix} PRIVMSG {target} :{text}", exclude=self)
            if self.bridge.mc:
                asyncio.create_task(self._send_chan_msg(idx, mc_text))
            else:
                self._bot_notice("MeshCore not connected")
            return

        repeater = self.bridge.repeater_sessions.get(target.lower())
        if repeater:
            self.send(f":{self.prefix} PRIVMSG {target} :{text}")
            if self.bridge.mc:
                asyncio.create_task(self._handle_repeater_cmd(repeater, text))
            else:
                self._repeater_msg(repeater, "MeshCore not connected")
            return

        contact = self.bridge.contact_for_nick(target)
        if contact:
            if contact.get('type', 0) != 2:
                max_len = 200
                byte_len = len(text.encode('utf-8'))
                if byte_len > max_len:
                    self.send(f":{target}!meshcore@meshcore PRIVMSG {self.nick} :Msg too long: {byte_len}/{max_len} (+{byte_len - max_len})")
                    return
            self.send(f":{self.prefix} PRIVMSG {target} :{text}")
            if contact.get('type', 0) == 2:
                # Repeater contact without an active session: still route to command
                # handler so that 'login <pwd>' works from the repeater's DM tab.
                if self.bridge.mc:
                    asyncio.create_task(self._handle_repeater_cmd(contact, text))
                else:
                    self._repeater_msg(contact, "MeshCore not connected")
            else:
                self.bridge.broadcast(f":{self.prefix} PRIVMSG {target} :{text}", exclude=self)
                if self.bridge.mc:
                    asyncio.create_task(self._send_dm(contact, text))
                else:
                    self._bot_notice("MeshCore not connected")
        else:
            self.numeric('401', target, ':No such nick/channel')

    async def _send_chan_msg(self, idx: int, mc_text: str):
        try:
            await self.bridge.mc.commands.send_chan_msg(idx, mc_text)
        except Exception as e:
            logger.error("send_chan_msg failed: %s", e)
            self._bot_notice(f"Channel send failed: {e}")

    def _contact_notice(self, contact: dict, text: str):
        """Broadcast a message that appears in the contact's DM tab on all clients."""
        nick = sanitize_nick(contact['adv_name'])
        pubkey = contact.get('public_key', '')
        self.bridge.broadcast(f":{nick}!{pubkey[:12] or 'mesh'}@meshcore PRIVMSG {self.nick} :{text}")

    async def _send_dm(self, contact: dict, text: str):
        try:
            ev = await self.bridge.mc.commands.send_msg_with_retry(contact, text)
            if ev is None:
                name = contact.get('adv_name', '?')
                logger.warning("send_dm: no ACK from %s", name)
                self._contact_notice(contact, f"Delivery failed: no ACK from {name}: {text}")
            elif ev.is_error():
                logger.error("send_dm failed: %s", ev.payload)
                self._contact_notice(contact, f"Send failed: {ev.payload}")
        except Exception as e:
            logger.error("send_dm exception: %s", e)
            self._contact_notice(contact, f"Send failed: {e}")

    def _bot_contact_line(self, c: dict, pubkey: str):
        nick = self.bridge.contact_nick(c)
        hops = c.get('out_path_len', -1)
        hop_str = f"hops={hops}" if hops >= 0 else "flood"
        self._bot_msg(f"  {nick:<20} [{pubkey[:12]}]  {hop_str}")

    def _bot_notice(self, text: str):
        self.send(f":{BOT_NICK}!bot@meshcore NOTICE {self.nick} :{text}")

    def _bot_msg(self, text: str):
        self.send(f":{BOT_NICK}!bot@meshcore PRIVMSG {self.nick} :{text}")

    def _web_base_url(self) -> str:
        cfg = self.bridge.config.get('webserver', {})
        if not cfg:
            return ''
        host = self.bridge.config['irc']['host']
        port = cfg.get('port', 8080)
        return cfg.get('url', f'http://{host}:{port}')

    def _map_url(self, map_type: str, sub: str) -> str:
        base = self._web_base_url()
        if not base:
            return ''
        return f"{base}/map/{map_type}/{urllib.parse.quote(sub, safe='')}"

    async def _handle_bot(self, text: str):
        parts = text.strip().split(' ', 2)
        cmd = parts[0].lower()

        if cmd == 'help':
            for line in [
                "── Companion ─────────────────────────────────────────",
                "  nodeinfo                     show our own node info",
                "  get                          show all node settings",
                "  get/set power [<dBm>]        TX power",
                "  get/set radio [<MHz> <kHz> <sf> <cr>]",
                "  get/set name [<name>]        node name",
                "  get/set coords [<lat> <lon>] GPS coordinates",
                "  get/set autoadd [<on|off>]   auto-add contacts",
                "  get/set lockey [<on|off>]    share GPS in advertisements",
                "  get/set multiack [<0-3>]     multi-ack count",
                "  get/set telemetry [<base|loc|env> <0-3>]",
                "  get/set af [<1-9>]           Airtime Factor",
                "  get/set pathmode [<1-4>]     path hash size (1=small, 4=large)",
                "  get tuning|bat|stats|deviceinfo|customs",
                "  refreshcontacts              refresh contact list from companion",
                "  zeroadvert / floodadvert     send self-advertisement",
                "  listchannels                 list all configured channel slots",
                "  addchannel <name>            join MeshCore channel (auto slot)",
                "  addchannel <idx> <name>      join channel at specific slot",
                "  deletechannel <name|#ch|idx> delete a channel from companion",
                "── Contacts ──────────────────────────────────────────",
                "  contacts <all|repeater|companion|sensor|room> [filter]",
                "  discovered <all|repeater|companion|sensor|room> [filter]",
                "  addcontact <nick|pubkey>     save a discovered contact",
                "  removecontact <nick|pubkey>  remove a saved contact from companion",
                "  renamecontact <nick|pubkey> <new name>  rename a saved contact",
                "  resetpath <nick|pubkey>      reset path to flood (auto-learn)",
                "  setpath <nick|pubkey> <hex>[:<mode>]  set fixed path (mode 0-3)",
                "  block <nick|pubkey>          ignore channel messages from a user",
                "  unblock <nick>               remove from block list",
                "  blocklist                    show block list",
                "── Telemetry ─────────────────────────────────────────",
                "  telemetry <nick>             request telemetry from a contact",
                "  telemetryallow <nick> <data|sensors|location|all>",
                "  telemetrydeny  <nick> <data|sensors|location|all>",
                "  telemetrylist                list contacts with telemetry permissions",
                "── Repeater ──────────────────────────────────────────",
                "  login <name> [pwd]           login (uses saved pwd if omitted)",
                "  logout <name>                logout from a repeater",
                "  synctime <name>              sync repeater clock to gateway time",
                "  status <name>                request repeater status",
                "  neighbours <name>            request repeater neighbours",
                "  expand [name]                fetch next batch of neighbours",
                "  savepassword <name> <pwd>    save repeater login password",
                "  deletepassword <name>        remove saved password",
                "  passwords                    list repeaters with saved passwords",
                "─────────────────────────────────────────────────────",
                "Tip: /whois <nick>  shows full contact details",
            ]:
                self._bot_msg(line)

        elif cmd == 'contacts':
            if len(parts) < 2:
                self._bot_msg("Usage: contacts <all|repeater|companion|sensor|room> [name/hash filter]")
                return
            filter_arg = parts[1].lower()
            if filter_arg not in _TYPE_FILTERS:
                self._bot_msg(f"Unknown type: {filter_arg}  —  options: all  repeater  companion  sensor  room")
                return
            name_filter = parts[2].lower() if len(parts) > 2 else ''
            allowed_types = _TYPE_FILTERS[filter_arg]

            mc = self.bridge.mc
            saved = mc.contacts if mc else {}
            if allowed_types is not None:
                saved = {k: v for k, v in saved.items() if v.get('type', 0) in allowed_types}
            if name_filter:
                saved = {k: v for k, v in saved.items()
                         if name_filter in self.bridge.contact_nick(v).lower()
                         or name_filter in k.lower()}
            if not saved:
                self._bot_msg("No saved contacts on companion — try: advert")
                return

            self._bot_msg(f"Contacts ({filter_arg}{', ' + name_filter if name_filter else ''}, {len(saved)}):")
            now = time.time()
            for pubkey, c in sorted(saved.items(), key=lambda kv: kv[1].get('adv_name', '').lower()):
                node_type = c.get('type', 0)
                type_label = _NODE_TYPE_LABEL.get(node_type, '?')
                nick = self.bridge.contact_nick(c)
                hops = c.get('out_path_len', -1)
                hop_str = f"  hops={hops}" if hops >= 0 else ""
                perm_str = _fmt_telem_perms(c.get('flags', 0))
                perm_str = f"  [{perm_str}]" if perm_str else ""
                ts = self.bridge.advert_last_ts_by_pubkey.get(pubkey, 0)
                adv_str = f"  adv:{_fmt_age(now - ts)}" if ts else "  adv:never"
                self._bot_msg(f"  [{type_label}] {nick:<22} [{pubkey[:12]}]{hop_str}{perm_str}{adv_str}")
            if not name_filter:
                url = self._map_url('contacts', filter_arg)
                if url:
                    self._bot_msg(f"Map: {url}")

        elif cmd == 'discovered':
            if len(parts) < 2:
                self._bot_msg("Usage: discovered <all|repeater|companion|sensor|room> [name/hash filter]")
                return
            filter_arg = parts[1].lower()
            if filter_arg not in _TYPE_FILTERS:
                self._bot_msg(f"Unknown type: {filter_arg}  —  options: all  repeater  companion  sensor  room")
                return
            name_filter = parts[2].lower() if len(parts) > 2 else ''
            allowed_types = _TYPE_FILTERS[filter_arg]

            mc = self.bridge.mc
            saved_keys = set(mc.contacts.keys()) if mc else set()

            # Merge: session-discovered contacts (unsaved) + advert cache (all, saved marked)
            merged: dict[str, dict] = {}
            for pubkey, c in self.bridge.contacts.items():
                if pubkey not in saved_keys:
                    merged[pubkey] = c
            cache = self.bridge.node_cache
            if cache:
                for pubkey, entry in cache.all_items():
                    if pubkey not in merged:
                        merged[pubkey] = {
                            'adv_name': entry.get('adv_name', ''),
                            'adv_lat':  entry.get('lat', 0.0),
                            'adv_lon':  entry.get('lon', 0.0),
                            'type':     entry.get('node_type', 0),
                            'public_key': pubkey,
                            '_saved': pubkey in saved_keys,
                        }

            if allowed_types is not None:
                merged = {k: v for k, v in merged.items()
                          if v.get('type', 0) in allowed_types}
            if name_filter:
                merged = {k: v for k, v in merged.items()
                          if name_filter in self.bridge.contact_nick(v).lower()
                          or name_filter in k.lower()}

            if not merged:
                self._bot_msg(f"No discovered {filter_arg} contacts" + (f" matching '{name_filter}'" if name_filter else ""))
                return

            self._bot_msg(f"Discovered ({filter_arg}{', ' + name_filter if name_filter else ''}, {len(merged)}):")
            now = time.time()
            for pubkey, c in sorted(merged.items(),
                                    key=lambda kv: kv[1].get('adv_name', '').lower()):
                node_type = c.get('type', 0)
                type_label = _NODE_TYPE_LABEL.get(node_type, '?')
                nick = self.bridge.contact_nick(c)
                hops = c.get('out_path_len', -1)
                hop_str = f"  hops={hops}" if hops >= 0 else ""
                saved_str = "  (saved)" if c.get('_saved') else ""
                ts = self.bridge.advert_last_ts_by_pubkey.get(pubkey, 0)
                adv_str = f"  adv:{_fmt_age(now - ts)}" if ts else "  adv:never"
                self._bot_msg(f"  [{type_label}] {nick:<22} [{pubkey[:12]}]{hop_str}{saved_str}{adv_str}")
            if not name_filter:
                url = self._map_url('discovered', filter_arg)
                if url:
                    self._bot_msg(f"Map: {url}")

        elif cmd == 'nodeinfo':
            si = self.bridge.self_info
            if not si:
                self._bot_msg("Node info not available yet")
                return
            self._bot_msg(f"Name:   {si.get('name', '?')}")
            self._bot_msg(f"Pubkey: {si.get('public_key', '?')}")
            lat = si.get('adv_lat', 0)
            lon = si.get('adv_lon', 0)
            if lat or lon:
                self._bot_msg(f"Pos:    {lat:.6f}, {lon:.6f}")
                self._bot_msg(f"Map:    {self.bridge.osm_link(lat, lon)}")
            self._bot_msg(
                f"Radio:  {si.get('radio_freq', '?')} MHz  "
                f"BW:{si.get('radio_bw', '?')} kHz  "
                f"SF:{si.get('radio_sf', '?')}  "
                f"CR:{si.get('radio_cr', '?')}"
            )

        elif cmd == 'get':
            sub = parts[1].lower() if len(parts) > 1 else ''
            asyncio.create_task(self._bot_get(sub))

        elif cmd == 'set':
            if len(parts) < 3:
                self._bot_msg("Usage: set <setting> <value>  —  try: help")
                return
            asyncio.create_task(self._bot_set(parts[1].lower(), parts[2].split()))

        elif cmd == 'login':
            if len(parts) < 2:
                self._bot_msg("Usage: login <name> [password]")
                return
            name = parts[1]
            pwd = parts[2] if len(parts) > 2 else ''
            contact = self.bridge.contact_for_nick(name)
            if not contact:
                self._bot_msg(f"Contact not found: {name}")
                return
            asyncio.create_task(self._bot_login(contact, pwd))

        elif cmd == 'logout':
            arg = ' '.join(parts[1:]).strip()
            if not arg:
                self._bot_msg("Usage: logout <name>")
                return
            contact = self.bridge.contact_for_nick(arg)
            if not contact:
                self._bot_msg(f"Contact not found: {arg}")
                return
            asyncio.create_task(self._bot_logout(contact))

        elif cmd == 'status':
            arg = ' '.join(parts[1:]).strip()
            if not arg:
                self._bot_msg("Usage: status <name>")
                return
            contact = self.bridge.contact_for_nick(arg)
            if not contact:
                self._bot_msg(f"Contact not found: {arg}")
                return
            asyncio.create_task(self._bot_status(contact))

        elif cmd == 'neighbours':
            arg = ' '.join(parts[1:]).strip()
            if not arg:
                self._bot_msg("Usage: neighbours <name>")
                return
            contact = self.bridge.contact_for_nick(arg)
            if not contact:
                self._bot_msg(f"Contact not found: {arg}")
                return
            asyncio.create_task(self._bot_neighbours(contact))

        elif cmd == 'synctime':
            arg = ' '.join(parts[1:]).strip()
            if not arg:
                self._bot_msg("Usage: synctime <name>")
                return
            contact = self.bridge.contact_for_nick(arg)
            if not contact:
                self._bot_msg(f"Contact not found: {arg}")
                return
            asyncio.create_task(self._bot_synctime(contact))

        elif cmd == 'expand':
            asyncio.create_task(self._bot_expand_neighbours(' '.join(parts[1:]).strip()))

        elif cmd == 'refreshcontacts':
            asyncio.create_task(self._bot_advert())

        elif cmd == 'zeroadvert':
            asyncio.create_task(self._bot_send_advert(flood=False))

        elif cmd == 'floodadvert':
            asyncio.create_task(self._bot_send_advert(flood=True))

        elif cmd == 'addchannel':
            if len(parts) < 2:
                self._bot_msg("Usage: addchannel <name>  or  addchannel <idx> <name>")
                return
            asyncio.create_task(self._bot_addchannel(parts[1], parts[2] if len(parts) > 2 else None))

        elif cmd == 'deletechannel':
            arg = ' '.join(parts[1:]).strip()
            if not arg:
                self._bot_msg("Usage: deletechannel <name|#channel|idx>")
                return
            asyncio.create_task(self._bot_deletechannel(arg))

        elif cmd == 'listchannels':
            self._bot_listchannels()

        elif cmd == 'addcontact':
            arg = ' '.join(parts[1:]).strip()
            if not arg:
                self._bot_msg("Usage: addcontact <nick>  or  addcontact <64-char pubkey>")
                return
            asyncio.create_task(self._bot_addcontact(arg))

        elif cmd == 'removecontact':
            arg = ' '.join(parts[1:]).strip()
            if not arg:
                self._bot_msg("Usage: removecontact <nick>  or  removecontact <pubkey-prefix>")
                return
            asyncio.create_task(self._bot_removecontact(arg))

        elif cmd == 'renamecontact':
            if len(parts) < 3:
                self._bot_msg("Usage: renamecontact <nick|pubkey> <new name>")
                return
            target = parts[1]
            new_name = ' '.join(parts[2:]).strip()
            asyncio.create_task(self._bot_renamecontact(target, new_name))

        elif cmd == 'resetpath':
            arg = ' '.join(parts[1:]).strip()
            if not arg:
                self._bot_msg("Usage: resetpath <nick|pubkey>")
                return
            asyncio.create_task(self._bot_resetpath(arg))

        elif cmd == 'setpath':
            if len(parts) < 3:
                self._bot_msg("Usage: setpath <nick|pubkey> <path_hex>[:<mode>]")
                return
            asyncio.create_task(self._bot_setpath(parts[1], parts[2]))

        elif cmd == 'telemetry':
            arg = ' '.join(parts[1:]).strip()
            if not arg:
                self._bot_msg("Usage: telemetry <nick|pubkey>")
                return
            contact = self.bridge.contact_for_nick(arg)
            if not contact:
                self._bot_msg(f"Contact not found: {arg}")
                return
            asyncio.create_task(self._bot_telemetry(contact))

        elif cmd == 'telemetryallow':
            allow_parts = ' '.join(parts[1:]).strip().split()
            if len(allow_parts) < 2:
                self._bot_msg("Usage: telemetryallow <nick|pubkey> <data|sensors|location|all>")
                return
            asyncio.create_task(self._bot_telemetryallow(allow_parts[0], allow_parts[1].lower()))

        elif cmd == 'telemetrydeny':
            deny_parts = ' '.join(parts[1:]).strip().split()
            if len(deny_parts) < 2:
                self._bot_msg("Usage: telemetrydeny <nick|pubkey> <data|sensors|location|all>")
                return
            asyncio.create_task(self._bot_telemetrydeny(deny_parts[0], deny_parts[1].lower()))

        elif cmd == 'telemetrylist':
            asyncio.create_task(self._bot_telemetrylist())

        elif cmd == 'block':
            arg = ' '.join(parts[1:]).strip()
            if not arg:
                self._bot_msg("Usage: block <nick|pubkey>")
                return
            asyncio.create_task(self._bot_block(arg))

        elif cmd == 'unblock':
            arg = ' '.join(parts[1:]).strip()
            if not arg:
                self._bot_msg("Usage: unblock <nick>")
                return
            asyncio.create_task(self._bot_unblock(arg))

        elif cmd == 'blocklist':
            asyncio.create_task(self._bot_blocklist())

        elif cmd == 'savepassword':
            if len(parts) < 3:
                self._bot_msg("Usage: savepassword <repeater> <password>")
                return
            name, pwd = parts[1], parts[2]
            contact = self.bridge.contact_for_nick(name)
            nick = sanitize_nick(contact['adv_name']) if contact else sanitize_nick(name)
            self._bot_savepassword(nick, pwd)

        elif cmd == 'deletepassword':
            arg = ' '.join(parts[1:]).strip()
            if not arg:
                self._bot_msg("Usage: deletepassword <repeater>")
                return
            contact = self.bridge.contact_for_nick(arg)
            nick = sanitize_nick(contact['adv_name']) if contact else sanitize_nick(arg)
            self._bot_deletepassword(nick)

        elif cmd == 'passwords':
            entries = self.bridge.password_list()
            if entries:
                self._bot_msg("Saved passwords: " + "  ".join(entries))
            else:
                self._bot_msg("No saved passwords")

        else:
            self._bot_msg(f"Unknown command: {cmd}  —  try: help")

    async def _bot_get(self, sub: str):
        mc = self.bridge.mc
        if not mc:
            self._bot_msg("MeshCore not connected")
            return
        _TM = {0: 'off', 1: 'on-request', 2: 'periodic', 3: 'periodic+request'}
        try:
            if sub in ('', 'settings'):
                ev = await mc.commands.send_appstart()
                si = ev.payload if (ev and not ev.is_error()) else self.bridge.self_info
                if ev and not ev.is_error():
                    self.bridge.self_info = si
                self._bot_msg(f"Name:       {si.get('name', '?')}")
                self._bot_msg(
                    f"Radio:      {si.get('radio_freq', '?')} MHz  "
                    f"BW:{si.get('radio_bw', '?')} kHz  "
                    f"SF:{si.get('radio_sf', '?')}  CR:{si.get('radio_cr', '?')}"
                )
                self._bot_msg(
                    f"TX power:   {si.get('tx_power', '?')} dBm  "
                    f"(max {si.get('max_tx_power', '?')} dBm)"
                )
                lat, lon = si.get('adv_lat', 0), si.get('adv_lon', 0)
                if lat or lon:
                    self._bot_msg(f"Coords:     {lat:.6f}, {lon:.6f}  {self.bridge.osm_link(lat, lon)}")
                else:
                    self._bot_msg("Coords:     not set")
                self._bot_msg(
                    f"Auto-add:   {'on' if not si.get('manual_add_contacts') else 'off (manual)'}"
                )
                self._bot_msg(
                    f"Loc in adv: {'on' if si.get('adv_loc_policy') else 'off'}"
                )
                self._bot_msg(f"Multi-ack:  {si.get('multi_acks', '?')}")
                self._bot_msg(
                    f"Telemetry:  base={_TM.get(si.get('telemetry_mode_base', 0), '?')}  "
                    f"loc={_TM.get(si.get('telemetry_mode_loc', 0), '?')}  "
                    f"env={_TM.get(si.get('telemetry_mode_env', 0), '?')}"
                )
                phm = si.get('path_hash_mode')
                if phm is not None:
                    phm_size = phm + 1
                    self._bot_msg(f"Path hash:  {phm_size} byte{'s' if phm_size != 1 else ''}  (mode {phm})")

            elif sub == 'bat':
                ev = await mc.commands.get_bat()
                if ev and not ev.is_error():
                    p = ev.payload
                    self._bot_msg(f"Battery: {p.get('level', '?')} mV")
                    if 'used_kb' in p:
                        self._bot_msg(f"Storage: {p['used_kb']} / {p['total_kb']} KB used")
                else:
                    self._bot_msg("Battery info not available")

            elif sub == 'tuning':
                ev = await mc.commands.get_tuning()
                if ev and not ev.is_error():
                    p = ev.payload
                    self._bot_msg(f"RX delay:       {p.get('rx_delay', '?')} ms")
                    self._bot_msg(f"Airtime factor: {p.get('airtime_factor', '?')}")
                else:
                    self._bot_msg("Tuning info not available")

            elif sub == 'deviceinfo':
                ev = await mc.commands.send_device_query()
                if ev and not ev.is_error():
                    for k, v in ev.payload.items():
                        self._bot_msg(f"  {k}: {v}")
                else:
                    self._bot_msg("Device info not available")

            elif sub == 'stats':
                for fn, label in [
                    (mc.commands.get_stats_core,    'Core'),
                    (mc.commands.get_stats_radio,   'Radio'),
                    (mc.commands.get_stats_packets, 'Packets'),
                ]:
                    ev = await fn()
                    if ev and not ev.is_error():
                        self._bot_msg(f"{label}:")
                        for k, v in ev.payload.items():
                            if v is not None:
                                self._bot_msg(f"  {k}: {v}")

            elif sub == 'pathmode':
                mode = await mc.commands.get_path_hash_mode()
                size = mode + 1
                self._bot_msg(f"Path hash size: {size} byte{'s' if size != 1 else ''}  (mode {mode})")

            elif sub in ('power', 'radio', 'name', 'coords', 'autoadd', 'lockey', 'multiack', 'telemetry'):
                ev = await mc.commands.send_appstart()
                si = ev.payload if (ev and not ev.is_error()) else self.bridge.self_info
                if ev and not ev.is_error():
                    self.bridge.self_info = si
                if sub == 'power':
                    self._bot_msg(f"TX power: {si.get('tx_power', '?')} dBm  (max {si.get('max_tx_power', '?')} dBm)")
                elif sub == 'radio':
                    self._bot_msg(
                        f"Radio: {si.get('radio_freq', '?')} MHz  "
                        f"BW:{si.get('radio_bw', '?')} kHz  "
                        f"SF:{si.get('radio_sf', '?')}  CR:{si.get('radio_cr', '?')}"
                    )
                elif sub == 'name':
                    self._bot_msg(f"Name: {si.get('name', '?')}")
                elif sub == 'coords':
                    lat, lon = si.get('adv_lat', 0), si.get('adv_lon', 0)
                    if lat or lon:
                        self._bot_msg(f"Coords: {lat:.6f}, {lon:.6f}")
                    else:
                        self._bot_msg("Coords: not set")
                elif sub == 'autoadd':
                    self._bot_msg(f"Auto-add: {'on' if not si.get('manual_add_contacts') else 'off (manual)'}")
                elif sub == 'lockey':
                    self._bot_msg(f"Loc in adv: {'on' if si.get('adv_loc_policy') else 'off'}")
                elif sub == 'multiack':
                    self._bot_msg(f"Multi-ack: {si.get('multi_acks', '?')}")
                elif sub == 'telemetry':
                    self._bot_msg(
                        f"Telemetry: base={_TM.get(si.get('telemetry_mode_base', 0), '?')}  "
                        f"loc={_TM.get(si.get('telemetry_mode_loc', 0), '?')}  "
                        f"env={_TM.get(si.get('telemetry_mode_env', 0), '?')}"
                    )

            elif sub == 'af':
                ev = await mc.commands.get_tuning()
                if ev and not ev.is_error():
                    self._bot_msg(f"Airtime Factor: {ev.payload.get('airtime_factor', '?')}")
                else:
                    self._bot_msg("Airtime Factor not available")

            elif sub == 'customs':
                ev = await mc.commands.get_custom_vars()
                if ev and not ev.is_error():
                    p = ev.payload
                    if p:
                        for k, v in p.items():
                            self._bot_msg(f"  {k} = {v}")
                    else:
                        self._bot_msg("No custom variables set")
                else:
                    self._bot_msg("Custom vars not available")

            else:
                self._bot_msg(
                    f"Unknown: get {sub}  —  options: (empty)  power  radio  name  coords  autoadd  lockey  multiack  telemetry  af  tuning  bat  stats  pathmode  deviceinfo  customs"
                )
        except Exception as e:
            self._bot_msg(f"Get error: {e}")

    async def _bot_set(self, setting: str, args: list):
        mc = self.bridge.mc
        if not mc:
            self._bot_msg("MeshCore not connected")
            return
        try:
            if setting == 'power':
                val = int(args[0])
                ev = await mc.commands.set_tx_power(val)
                self._bot_msg(f"TX power set to {val} dBm" if ev and not ev.is_error()
                              else "Failed to set TX power")

            elif setting == 'radio':
                if len(args) < 4:
                    self._bot_msg("Usage: set radio <freq_MHz> <bw_kHz> <sf> <cr>")
                    return
                freq, bw, sf, cr = float(args[0]), float(args[1]), int(args[2]), int(args[3])
                ev = await mc.commands.set_radio(freq, bw, sf, cr)
                self._bot_msg(
                    f"Radio set: {freq} MHz  BW:{bw} kHz  SF:{sf}  CR:{cr}"
                    if ev and not ev.is_error() else "Failed to set radio"
                )

            elif setting == 'name':
                name = ' '.join(args)
                ev = await mc.commands.set_name(name)
                self._bot_msg(f"Name set to: {name}" if ev and not ev.is_error()
                              else "Failed to set name")

            elif setting == 'coords':
                if len(args) < 2:
                    self._bot_msg("Usage: set coords <lat> <lon>")
                    return
                lat, lon = float(args[0]), float(args[1])
                ev = await mc.commands.set_coords(lat, lon)
                self._bot_msg(f"Coords set: {lat:.6f}, {lon:.6f}" if ev and not ev.is_error()
                              else "Failed to set coords")

            elif setting == 'autoadd':
                on = args[0].lower() in ('on', '1', 'true', 'yes')
                ev = await mc.commands.set_manual_add_contacts(not on)
                self._bot_msg(f"Auto-add contacts: {'on' if on else 'off'}"
                              if ev and not ev.is_error() else "Failed to set auto-add")

            elif setting == 'lockey':
                on = args[0].lower() in ('on', '1', 'true', 'yes')
                ev = await mc.commands.set_advert_loc_policy(1 if on else 0)
                self._bot_msg(f"Location in adverts: {'on' if on else 'off'}"
                              if ev and not ev.is_error() else "Failed to set location policy")

            elif setting == 'multiack':
                val = int(args[0])
                ev = await mc.commands.set_multi_acks(val)
                self._bot_msg(f"Multi-ack set to {val}" if ev and not ev.is_error()
                              else "Failed to set multi-ack")

            elif setting == 'telemetry':
                if len(args) < 2:
                    self._bot_msg("Usage: set telemetry <base|loc|env> <0-3>")
                    self._bot_msg("  0=off  1=on-request  2=periodic  3=periodic+request")
                    return
                which, val = args[0].lower(), int(args[1])
                fns = {
                    'base': mc.commands.set_telemetry_mode_base,
                    'loc':  mc.commands.set_telemetry_mode_loc,
                    'env':  mc.commands.set_telemetry_mode_env,
                }
                if which not in fns:
                    self._bot_msg("Usage: set telemetry <base|loc|env> <0-3>")
                    return
                ev = await fns[which](val)
                self._bot_msg(f"Telemetry {which} set to {val}" if ev and not ev.is_error()
                              else f"Failed to set telemetry {which}")

            elif setting == 'af':
                if not args:
                    self._bot_msg("Usage: set af <1-9>")
                    return
                af_val = int(args[0])
                if af_val < 1 or af_val > 9:
                    self._bot_msg("Airtime Factor must be between 1 and 9")
                    return
                tev = await mc.commands.get_tuning()
                rx_dly = tev.payload.get('rx_delay', 0) if tev and not tev.is_error() else 0
                ev = await mc.commands.set_tuning(rx_dly, af_val)
                self._bot_msg(
                    f"Airtime Factor set to {af_val}  (rx_delay unchanged: {rx_dly} ms)"
                    if ev and not ev.is_error() else "Failed to set Airtime Factor"
                )

            elif setting == 'tuning':
                if len(args) < 2:
                    self._bot_msg("Usage: set tuning <rx_delay_ms> <airtime_factor>")
                    return
                ev = await mc.commands.set_tuning(int(args[0]), int(args[1]))
                self._bot_msg(
                    f"Tuning set: rx_delay={args[0]} ms  airtime_factor={args[1]}"
                    if ev and not ev.is_error() else "Failed to set tuning"
                )

            elif setting == 'pathmode':
                if not args:
                    self._bot_msg("Usage: set pathmode <1-4>  (bytes per path hash; use: get pathmode)")
                    return
                size = int(args[0])
                if size < 1 or size > 4:
                    self._bot_msg("Path hash size must be 1, 2, 3 or 4 bytes")
                    return
                mode = size - 1
                ev = await mc.commands.set_path_hash_mode(mode)
                self._bot_msg(
                    f"Path hash size set to {size} byte{'s' if size != 1 else ''}  (mode {mode})"
                    if ev and not ev.is_error() else "Failed to set path hash mode"
                )

            else:
                self._bot_msg(
                    f"Unknown setting: {setting}  —  "
                    "options: power  radio  name  coords  autoadd  lockey  multiack  telemetry  af  tuning  pathmode"
                )

        except (ValueError, IndexError) as e:
            self._bot_msg(f"Invalid value: {e}")
        except Exception as e:
            self._bot_msg(f"Set error: {e}")

    def _repeater_msg(self, contact: dict, text: str):
        nick = sanitize_nick(contact['adv_name'])
        pubkey = contact.get('public_key', '')
        self.bridge.broadcast(f":{nick}!{pubkey[:12] or 'repeater'}@meshcore PRIVMSG {self.nick} :{text}")

    async def _bot_login(self, contact: dict, pwd: str, reply_fn=None):
        if reply_fn is None:
            reply_fn = self._bot_msg
        if not pwd:
            pwd = self.bridge.password_get(sanitize_nick(contact['adv_name']))
        try:
            event = await self.bridge.mc.commands.send_login_sync(contact, pwd, timeout=10)
            if event:
                role = 'admin' if event.payload.get('is_admin') else 'read-only'
                nick = sanitize_nick(contact['adv_name'])
                self.bridge.repeater_sessions[nick.lower()] = contact
                reply_fn(f"Logged in to {contact['adv_name']} ({role})")
                self._repeater_msg(contact, f"Session open ({role}) — commands: status  neighbours  expand  synctime  telemetry  advert  zeroadvert  resetpath  cli <cmd>  logout")
            else:
                reply_fn(f"Login to {contact['adv_name']} failed or timed out")
        except Exception as e:
            reply_fn(f"Login error: {e}")

    async def _bot_logout(self, contact: dict, reply_fn=None):
        if reply_fn is None:
            reply_fn = self._bot_msg
        try:
            await self.bridge.mc.commands.send_logout(contact)
            nick = sanitize_nick(contact['adv_name'])
            self.bridge.repeater_sessions.pop(nick.lower(), None)
            self._repeater_msg(contact, "Session closed")
            reply_fn(f"Logged out from {contact['adv_name']}")
        except Exception as e:
            reply_fn(f"Logout error: {e}")

    async def _bot_cli(self, contact: dict, command: str, reply_fn=None):
        if reply_fn is None:
            reply_fn = lambda m: self._repeater_msg(contact, m)
        try:
            ev = await self.bridge.mc.commands.send_cmd(contact, command)
            if ev and ev.is_error():
                reply_fn(f"CLI send failed: {ev.payload.get('reason', '?')}")
            # Response arrives as a DM from the repeater via _on_contact_msg
        except Exception as e:
            reply_fn(f"CLI error: {e}")

    async def _bot_synctime(self, contact: dict, reply_fn=None):
        import time as _time
        if reply_fn is None:
            reply_fn = lambda m: self._repeater_msg(contact, m)
        mc = self.bridge.mc
        if not mc:
            reply_fn("MeshCore not connected")
            return
        try:
            ts = int(_time.time())
            ev = await mc.commands.send_cmd(contact, f"time {ts}")
            if ev and ev.is_error():
                reply_fn(f"Time sync failed: {ev.payload.get('reason', '?')}")
            else:
                dt = datetime.datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                reply_fn(f"Time sync sent: {dt}")
        except Exception as e:
            reply_fn(f"Time sync error: {e}")

    async def _bot_repeater_advert(self, contact: dict, flood: bool, reply_fn=None):
        if reply_fn is None:
            reply_fn = lambda m: self._repeater_msg(contact, m)
        mc = self.bridge.mc
        if not mc:
            reply_fn("MeshCore not connected")
            return
        cmd_str = "advert" if flood else "advert.zerohop"
        try:
            ev = await mc.commands.send_cmd(contact, cmd_str)
            if ev and ev.is_error():
                reply_fn(f"Advert failed: {ev.payload.get('reason', '?')}")
            else:
                kind = "flood" if flood else "zero-hop"
                reply_fn(f"Advert sent ({kind})")
        except Exception as e:
            reply_fn(f"Advert error: {e}")

    def _bot_savepassword(self, nick: str, pwd: str, reply_fn=None):
        if reply_fn is None:
            reply_fn = self._bot_msg
        self.bridge.password_set(nick, pwd)
        reply_fn(f"Password saved for {nick}")

    def _bot_deletepassword(self, nick: str, reply_fn=None):
        if reply_fn is None:
            reply_fn = self._bot_msg
        if self.bridge.password_delete(nick):
            reply_fn(f"Saved password removed for {nick}")
        else:
            reply_fn(f"No saved password for {nick}")

    async def _handle_repeater_cmd(self, contact: dict, text: str):
        parts = text.strip().split(None, 1)
        cmd = parts[0].lower() if parts else ''
        arg = parts[1] if len(parts) > 1 else ''
        reply = lambda m: self._repeater_msg(contact, m)
        if cmd == 'status':
            await self._bot_status(contact, reply_fn=reply)
        elif cmd == 'neighbours':
            await self._bot_neighbours(contact, reply_fn=reply)
        elif cmd == 'expand':
            await self._bot_expand_neighbours(reply_fn=reply)
        elif cmd == 'login':
            await self._bot_login(contact, arg, reply_fn=reply)
        elif cmd == 'logout':
            await self._bot_logout(contact, reply_fn=reply)
        elif cmd == 'cli':
            if not arg:
                self._repeater_msg(contact, "Usage: cli <command>")
                return
            await self._bot_cli(contact, arg, reply_fn=reply)
        elif cmd == 'synctime':
            await self._bot_synctime(contact, reply_fn=reply)
        elif cmd == 'telemetry':
            await self._bot_telemetry(contact, reply_fn=reply)
        elif cmd == 'zeroadvert':
            await self._bot_repeater_advert(contact, flood=False, reply_fn=reply)
        elif cmd in ('advert', 'floodadvert'):
            await self._bot_repeater_advert(contact, flood=True, reply_fn=reply)
        elif cmd == 'savepassword':
            if not arg:
                self._repeater_msg(contact, "Usage: savepassword <password>")
                return
            nick = sanitize_nick(contact['adv_name'])
            self._bot_savepassword(nick, arg, reply_fn=reply)
        elif cmd == 'deletepassword':
            nick = sanitize_nick(contact['adv_name'])
            self._bot_deletepassword(nick, reply_fn=reply)
        elif cmd == 'resetpath':
            asyncio.create_task(self._bot_resetpath(contact=contact, reply_fn=reply))
        else:
            self._repeater_msg(contact, "Commands: login [pwd]  logout  synctime  telemetry  advert  zeroadvert  status  neighbours  expand  resetpath  savepassword <pwd>  deletepassword  cli <cmd>")

    async def _bot_status(self, contact: dict, reply_fn=None):
        if reply_fn is None:
            reply_fn = self._bot_msg
        try:
            payload = await self.bridge.mc.commands.req_status_sync(contact, timeout=15)
            if payload is not None:
                reply_fn(f"Status from {contact['adv_name']}:")
                if isinstance(payload, dict):
                    for k, v in payload.items():
                        if k != 'pubkey_pre':
                            reply_fn(f"  {k}: {v}")
                else:
                    reply_fn(f"  {payload}")
            else:
                reply_fn(f"No status response from {contact['adv_name']} (timeout)")
        except Exception as e:
            reply_fn(f"Status error: {e}")

    def _resolve_neighbour(self, pubkey: str, cache):
        """Resolve a neighbour pubkey prefix to (name, node_type, lat, lon)."""
        c = (self.bridge.contact_for_pubkey_prefix(pubkey)
             or (self.bridge.mc and next(
                 (v for k, v in self.bridge.mc.contacts.items()
                  if k.lower().startswith(pubkey.lower())), None)))
        cache_entry = cache.get_by_prefix(pubkey) if cache else None
        if c:
            name = sanitize_nick(c.get('adv_name', pubkey))
            node_type = c.get('type', 0)
            lat = c.get('adv_lat', 0) or (cache_entry.get('lat', 0) if cache_entry else 0)
            lon = c.get('adv_lon', 0) or (cache_entry.get('lon', 0) if cache_entry else 0)
        elif cache_entry:
            name = sanitize_nick(cache_entry.get('adv_name', pubkey))
            node_type = cache_entry.get('node_type', 0)
            lat = cache_entry.get('lat', 0)
            lon = cache_entry.get('lon', 0)
        else:
            name = pubkey or '?'
            node_type = 0
            lat = lon = 0
        return name, node_type, lat, lon

    async def _bot_neighbours(self, contact: dict, reply_fn=None):
        if reply_fn is None:
            reply_fn = self._bot_msg
        cache = self.bridge.node_cache
        key = sanitize_nick(contact['adv_name'])
        nick_lower = key.lower()
        try:
            payload = await self.bridge.mc.commands.req_neighbours_sync(
                contact, count=255, offset=0, timeout=20)
            if payload is None:
                reply_fn(f"No neighbours response from {contact['adv_name']} (timeout)")
                return

            neighbours = payload.get('neighbours', [])
            total = payload.get('neighbours_count', len(neighbours))
            fetched = payload.get('results_count', len(neighbours))
            reply_fn(f"Neighbours of {contact['adv_name']} ({fetched} of {total}):")

            rep_entry = cache.get_by_pubkey(contact.get('public_key', '')) if cache else None
            rep_lat = rep_entry.get('lat', 0) if rep_entry else 0
            rep_lon = rep_entry.get('lon', 0) if rep_entry else 0

            now = time.time()
            map_nodes = []
            for n in neighbours:
                pubkey = n.get('pubkey', '')
                secs = n.get('secs_ago')
                snr = n.get('snr')
                snr_str = f"{snr:.1f}" if snr is not None else '?'
                name, node_type, lat, lon = self._resolve_neighbour(pubkey, cache)
                ts = int(now - secs) if secs is not None else None
                reply_fn(f"  {name}  {_fmt_age(secs)}  SNR:{snr_str}")
                if lat or lon:
                    map_nodes.append({'name': name, 'lat': lat, 'lon': lon,
                                      'type': node_type, 'snr': snr, 'ts': ts})

            neighbours_store.save(key, contact['adv_name'], map_nodes, rep_lat, rep_lon)
            url = self._map_url('neighbours', key)
            if url:
                reply_fn(f"Map: {url}")

            if fetched < total:
                self._neighbours_pending[nick_lower] = {
                    'contact': contact, 'offset': fetched, 'total': total, 'key': key,
                }
                self._last_neighbours_nick = nick_lower
                reply_fn(f"  ({total - fetched} more — use 'expand' to fetch next batch)")
            else:
                self._neighbours_pending.pop(nick_lower, None)

        except Exception as e:
            reply_fn(f"Neighbours error: {e}")

    async def _bot_expand_neighbours(self, name: str = '', reply_fn=None):
        if reply_fn is None:
            reply_fn = self._bot_msg

        if name:
            contact = self.bridge.contact_for_nick(name)
            if not contact:
                reply_fn(f"Contact not found: {name}")
                return
            nick_lower = sanitize_nick(contact['adv_name']).lower()
        else:
            nick_lower = self._last_neighbours_nick
            if not nick_lower:
                reply_fn("No pending neighbours query — use 'neighbours <name>' first")
                return

        state = self._neighbours_pending.get(nick_lower)
        if not state:
            reply_fn(f"No more neighbours pending for {nick_lower} — already complete")
            return

        contact = state['contact']
        offset = state['offset']
        total = state['total']
        key = state['key']
        cache = self.bridge.node_cache

        try:
            payload = await self.bridge.mc.commands.req_neighbours_sync(
                contact, count=255, offset=offset, timeout=20)
            if payload is None:
                reply_fn(f"No response from {contact['adv_name']} (timeout)")
                return

            neighbours = payload.get('neighbours', [])
            fetched_now = payload.get('results_count', len(neighbours))
            new_offset = offset + fetched_now
            reply_fn(f"More neighbours of {contact['adv_name']} "
                     f"({fetched_now} more, {new_offset} of {total} total):")

            existing = neighbours_store.load(key)
            existing_nodes = existing.get('nodes', []) if existing else []
            rep_lat = existing.get('repeater_lat', 0.0) if existing else 0.0
            rep_lon = existing.get('repeater_lon', 0.0) if existing else 0.0

            now = time.time()
            new_map_nodes = []
            for n in neighbours:
                pubkey = n.get('pubkey', '')
                secs = n.get('secs_ago')
                snr = n.get('snr')
                snr_str = f"{snr:.1f}" if snr is not None else '?'
                name_r, node_type, lat, lon = self._resolve_neighbour(pubkey, cache)
                ts = int(now - secs) if secs is not None else None
                reply_fn(f"  {name_r}  {_fmt_age(secs)}  SNR:{snr_str}")
                if lat or lon:
                    new_map_nodes.append({'name': name_r, 'lat': lat, 'lon': lon,
                                         'type': node_type, 'snr': snr, 'ts': ts})

            all_nodes = existing_nodes + new_map_nodes
            neighbours_store.save(key, contact['adv_name'], all_nodes, rep_lat, rep_lon)
            url = self._map_url('neighbours', key)
            if url:
                reply_fn(f"Map updated: {url}")

            if new_offset < total:
                state['offset'] = new_offset
                reply_fn(f"  ({total - new_offset} more — use 'expand' again)")
            else:
                self._neighbours_pending.pop(nick_lower, None)
                if nick_lower == self._last_neighbours_nick:
                    self._last_neighbours_nick = ''

        except Exception as e:
            reply_fn(f"Expand error: {e}")

    async def _bot_advert(self):
        try:
            await self.bridge.mc.ensure_contacts()
            self.bridge.contacts = dict(self.bridge.mc.contacts)
            self._bot_msg(f"Contacts refreshed: {len(self.bridge.contacts)} known")
        except Exception as e:
            self._bot_msg(f"Refresh error: {e}")

    async def _bot_send_advert(self, flood: bool):
        if not self.bridge.mc:
            self._bot_msg("MeshCore not connected")
            return
        try:
            ev = await self.bridge.mc.commands.send_advert(flood=flood)
            if ev and not ev.is_error():
                kind = "flood" if flood else "zero-hop"
                self._bot_msg(f"Self-advertisement sent ({kind})")
            else:
                self._bot_msg("Advertisement send failed")
        except Exception as e:
            self._bot_msg(f"Advertisement error: {e}")

    async def _bot_addchannel(self, arg1: str, arg2: str | None):
        if not self.bridge.mc:
            self._bot_msg("MeshCore not connected")
            return

        if arg2 is not None:
            try:
                idx = int(arg1)
            except ValueError:
                self._bot_msg("Usage: addchannel <name>  or  addchannel <idx> <name>")
                return
            name = arg2.strip()
        else:
            name = arg1.strip()
            idx = next((i for i in range(8) if i not in self.bridge.channels), None)
            if idx is None:
                self._bot_msg("All channel slots 0-7 are in use")
                return

        if not name:
            self._bot_msg("Channel name cannot be empty")
            return

        try:
            ev = await self.bridge.mc.commands.set_channel(idx, name)
            if ev and not ev.is_error():
                self.bridge.channels[idx] = name
                irc_channel = self.bridge.irc_channel_for_idx(idx)
                self.bridge.join_all_clients_to_channel(irc_channel)
                self._bot_msg(f"Channel added: {name} → {irc_channel} (slot {idx})")
            else:
                self._bot_msg(f"Failed to set channel slot {idx}")
        except Exception as e:
            self._bot_msg(f"addchannel error: {e}")

    async def _bot_deletechannel(self, arg: str):
        if not self.bridge.mc:
            self._bot_msg("MeshCore not connected")
            return

        # Resolve arg to a slot index
        idx = None
        # Try numeric slot
        try:
            candidate = int(arg)
            if candidate in self.bridge.channels:
                idx = candidate
            else:
                self._bot_msg(f"No channel in slot {candidate}")
                return
        except ValueError:
            # Try IRC channel name or bare name match
            arg_lower = arg.lower().lstrip('#')
            for i, name in self.bridge.channels.items():
                if (self.bridge.irc_channel_for_idx(i).lower().lstrip('#') == arg_lower
                        or name.lower() == arg_lower):
                    idx = i
                    break
            if idx is None:
                self._bot_msg(f"Channel not found: {arg}")
                return

        irc_channel = self.bridge.irc_channel_for_idx(idx)
        chan_name = self.bridge.channels[idx]
        try:
            ev = await self.bridge.mc.commands.set_channel(idx, '')
            if ev and not ev.is_error():
                del self.bridge.channels[idx]
                self.bridge.part_all_clients_from_channel(irc_channel, 'Channel deleted')
                self._bot_msg(f"Channel deleted: {chan_name} (slot {idx})")
            else:
                self._bot_msg(f"Failed to delete channel slot {idx}")
        except Exception as e:
            self._bot_msg(f"deletechannel error: {e}")

    def _bot_listchannels(self):
        if not self.bridge.channels:
            self._bot_msg("No channels configured")
            return
        for idx in sorted(self.bridge.channels):
            name = self.bridge.channels[idx]
            irc_ch = self.bridge.irc_channel_for_idx(idx)
            self._bot_msg(f"  [{idx}] {name} → {irc_ch}")

    async def _bot_addcontact(self, arg: str):
        arg = arg.strip()
        arg_lower = arg.lower()

        # 1. bridge.contacts by nick
        contact = self.bridge.contact_for_nick(arg)
        # 2. bridge.contacts by full pubkey
        if not contact and len(arg_lower) == 64 and all(c in '0123456789abcdef' for c in arg_lower):
            contact = self.bridge.contacts.get(arg_lower)
        # 3. advert cache by pubkey prefix or name
        if not contact:
            cache = self.bridge.node_cache
            if cache:
                found_pubkey, found_entry = None, None
                for pubkey, e in cache.all_items():
                    if pubkey.lower().startswith(arg_lower):
                        found_pubkey, found_entry = pubkey, e
                        break
                if not found_entry:
                    arg_nick = sanitize_nick(arg).lower()
                    for pubkey, e in cache.all_items():
                        if sanitize_nick(e.get('adv_name', '')).lower() == arg_nick:
                            found_pubkey, found_entry = pubkey, e
                            break
                if found_entry:
                    contact = {
                        'public_key': found_pubkey,
                        'adv_name': found_entry.get('adv_name', ''),
                        'type': found_entry.get('node_type', 0),
                        'flags': 0,
                        'out_path': found_entry.get('out_path', '0' * 128),
                        'out_path_len': found_entry.get('out_path_len', -1),
                        'out_path_hash_mode': found_entry.get('out_path_hash_mode', 0),
                        'last_advert': int(found_entry.get('last_seen', 0)),
                        'adv_lat': found_entry.get('lat', 0.0),
                        'adv_lon': found_entry.get('lon', 0.0),
                    }
        if not contact:
            self._bot_msg(f"Contact not found: {arg}  (try: discovered)")
            return

        if not self.bridge.mc:
            self._bot_msg("MeshCore not connected")
            return

        pubkey = contact.get('public_key', '')
        name = contact.get('adv_name', arg)
        nick = sanitize_nick(name)

        if pubkey in self.bridge.mc.contacts:
            self._bot_msg(f"Already saved: {name} [{pubkey[:12]}]  —  /msg {nick} <text>")
            return

        try:
            ev = await self.bridge.mc.commands.add_contact(contact)
            if ev and not ev.is_error():
                self.bridge.mc.contacts[pubkey] = contact
                self._bot_msg(f"Saved to companion: {name} [{pubkey[:12]}]  —  /msg {nick} <text>")
            else:
                self._bot_msg(f"Failed to save contact: {name}")
        except Exception as e:
            self._bot_msg(f"addcontact error: {e}")

    async def _bot_removecontact(self, arg: str):
        arg = arg.strip()

        # Resolve by nick, full pubkey, or pubkey prefix
        contact = self.bridge.contact_for_nick(arg)
        if not contact:
            arg_lower = arg.lower()
            for pubkey, c in self.bridge.contacts.items():
                if pubkey.lower().startswith(arg_lower) or pubkey.lower() == arg_lower:
                    contact = c
                    break
        if not contact:
            self._bot_msg(f"Contact not found: {arg}  (try: contacts)")
            return

        if not self.bridge.mc:
            self._bot_msg("MeshCore not connected")
            return

        pubkey = contact.get('public_key', '')
        name = contact.get('adv_name', arg)

        if pubkey not in self.bridge.mc.contacts:
            self._bot_msg(f"{name} [{pubkey[:12]}] is not a saved contact on the companion")
            return

        try:
            ev = await self.bridge.mc.commands.remove_contact(bytes.fromhex(pubkey))
            if ev and not ev.is_error():
                self.bridge.mc.contacts.pop(pubkey, None)
                self.bridge.contacts.pop(pubkey, None)
                self._bot_msg(f"Removed from companion: {name} [{pubkey[:12]}]")
            else:
                self._bot_msg(f"Failed to remove contact: {name}")
        except Exception as e:
            self._bot_msg(f"removecontact error: {e}")

    async def _bot_renamecontact(self, arg: str, new_name: str):
        arg = arg.strip()
        new_name = new_name.strip()
        if not new_name:
            self._bot_msg("Usage: renamecontact <nick|pubkey> <new name>")
            return

        # Resolve by nick or pubkey prefix from saved contacts
        contact = self.bridge.contact_for_nick(arg)
        if not contact:
            arg_lower = arg.lower()
            for pubkey, c in self.bridge.contacts.items():
                if pubkey.lower().startswith(arg_lower):
                    contact = c
                    break
        if not contact:
            self._bot_msg(f"Contact not found: {arg}  (try: contacts)")
            return

        if not self.bridge.mc:
            self._bot_msg("MeshCore not connected")
            return

        pubkey = contact.get('public_key', '')
        if pubkey not in self.bridge.mc.contacts:
            self._bot_msg(f"{contact.get('adv_name', arg)} [{pubkey[:12]}] is not a saved contact on the companion")
            return

        old_name = contact.get('adv_name', arg)
        contact = dict(contact)
        contact['adv_name'] = new_name

        try:
            ev = await self.bridge.mc.commands.update_contact(contact)
            if ev and not ev.is_error():
                self.bridge.mc.contacts[pubkey] = contact
                self.bridge.contacts[pubkey] = contact
                if self.bridge.node_cache:
                    self.bridge.node_cache.update(contact)
                    self.bridge.node_cache.flush()
                host = pubkey[:12] or 'mesh'
                result = self.bridge.rename_contact_nick(old_name, new_name, host=host)
                if result:
                    old_nick, new_nick = result
                    self._bot_msg(f"Renamed: {old_nick} → {new_nick} [{pubkey[:12]}]")
                else:
                    self._bot_msg(f"Renamed: {old_name} → {new_name} [{pubkey[:12]}]")
            else:
                self._bot_msg(f"Failed to rename contact: {old_name}")
        except Exception as e:
            self._bot_msg(f"renamecontact error: {e}")

    def _resolve_contact_for_path_cmd(self, arg: str):
        contact = self.bridge.contact_for_nick(arg)
        if not contact:
            arg_lower = arg.lower()
            for pubkey, c in self.bridge.contacts.items():
                if pubkey.lower().startswith(arg_lower):
                    contact = c
                    break
        return contact

    async def _bot_resetpath(self, arg: str = '', contact: dict = None, reply_fn=None):
        if reply_fn is None:
            reply_fn = self._bot_msg
        if contact is None:
            contact = self._resolve_contact_for_path_cmd(arg)
        if not contact:
            reply_fn(f"Contact not found: {arg}  (try: contacts)")
            return
        if not self.bridge.mc:
            reply_fn("MeshCore not connected")
            return
        pubkey = contact.get('public_key', '')
        name = contact.get('adv_name', arg)
        if pubkey not in self.bridge.mc.contacts:
            reply_fn(f"{name} [{pubkey[:12]}] is not a saved contact on the companion")
            return
        try:
            ev = await self.bridge.mc.commands.reset_path(bytes.fromhex(pubkey))
            if ev and not ev.is_error():
                contact = dict(contact)
                contact['out_path_len'] = -1
                contact['out_path'] = '0' * 128
                self.bridge.mc.contacts[pubkey] = contact
                self.bridge.contacts[pubkey] = contact
                if self.bridge.node_cache:
                    self.bridge.node_cache.update(contact)
                    self.bridge.node_cache.flush()
                reply_fn(f"Path reset (flood) for {name} [{pubkey[:12]}]")
            else:
                reason = ev.payload.get('reason', '?') if ev else 'no response'
                reply_fn(f"resetpath failed for {name}: {reason}")
        except Exception as e:
            reply_fn(f"resetpath error: {e}")

    async def _bot_setpath(self, arg: str, path_arg: str):
        contact = self._resolve_contact_for_path_cmd(arg)
        if not contact:
            self._bot_msg(f"Contact not found: {arg}  (try: contacts)")
            return
        if not self.bridge.mc:
            self._bot_msg("MeshCore not connected")
            return
        pubkey = contact.get('public_key', '')
        name = contact.get('adv_name', arg)
        if pubkey not in self.bridge.mc.contacts:
            self._bot_msg(f"{name} [{pubkey[:12]}] is not a saved contact on the companion")
            return
        # Validate hex (strip optional :mode suffix for the check)
        hex_part = path_arg.split(':')[0]
        if not hex_part or not all(c in '0123456789abcdefABCDEF' for c in hex_part):
            self._bot_msg("Path must be a hex string, e.g. d3810a51 or d3810a51:1")
            return
        try:
            contact = dict(contact)
            ev = await self.bridge.mc.commands.change_contact_path(contact, path_arg)
            if ev and not ev.is_error():
                self.bridge.mc.contacts[pubkey] = contact
                self.bridge.contacts[pubkey] = contact
                if self.bridge.node_cache:
                    self.bridge.node_cache.update(contact)
                    self.bridge.node_cache.flush()
                hops = contact.get('out_path_len', '?')
                mode = contact.get('out_path_hash_mode', '?')
                self._bot_msg(f"Path set for {name} [{pubkey[:12]}]: {hops} hop(s) mode={mode}")
            else:
                reason = ev.payload.get('reason', '?') if ev else 'no response'
                self._bot_msg(f"setpath failed for {name}: {reason}")
        except Exception as e:
            self._bot_msg(f"setpath error: {e}")

    async def _bot_telemetry(self, contact: dict, reply_fn=None):
        if reply_fn is None:
            reply_fn = self._bot_msg
        mc = self.bridge.mc
        if not mc:
            reply_fn("MeshCore not connected")
            return
        name = contact.get('adv_name', '?')
        try:
            lpp = await mc.commands.req_telemetry_sync(contact, timeout=20)
            if lpp is None:
                reply_fn(f"No telemetry response from {name} (timeout)")
                return
            reply_fn(f"Telemetry from {name}:")
            _fmt_lpp(lpp, reply_fn)
        except Exception as e:
            reply_fn(f"Telemetry error: {e}")

    def _resolve_contact_for_flags(self, arg: str):
        """Resolve nick/pubkey to a saved contact dict (must be in mc.contacts for flag changes)."""
        contact = self.bridge.contact_for_nick(arg)
        if not contact:
            arg_lower = arg.lower()
            for pubkey, c in self.bridge.contacts.items():
                if pubkey.lower().startswith(arg_lower):
                    contact = c
                    break
        if not contact:
            return None, f"Contact not found: {arg}  (try: contacts all)"
        mc = self.bridge.mc
        if not mc:
            return None, "MeshCore not connected"
        pubkey = contact.get('public_key', '')
        if pubkey not in mc.contacts:
            return None, f"{contact.get('adv_name', arg)} [{pubkey[:12]}] is not a saved contact"
        return mc.contacts[pubkey], None

    async def _bot_telemetryallow(self, arg: str, perm: str):
        bits = _TELEM_PERM_MAP.get(perm)
        if bits is None:
            self._bot_msg(f"Unknown permission '{perm}'  —  use: data  sensors  location  all")
            return
        contact, err = self._resolve_contact_for_flags(arg)
        if err:
            self._bot_msg(err)
            return
        name = contact.get('adv_name', arg)
        pubkey = contact.get('public_key', '')
        new_flags = contact.get('flags', 0) | bits
        try:
            ev = await self.bridge.mc.commands.change_contact_flags(contact, new_flags)
            if ev and not ev.is_error():
                self.bridge.contacts[pubkey] = contact  # reflect in bridge
                self._bot_msg(f"Telemetry {perm} allowed: {name} [{pubkey[:12]}]  flags=0x{new_flags:02x}  {_fmt_telem_perms(new_flags)}")
            else:
                self._bot_msg(f"Failed to update flags for {name}")
        except Exception as e:
            self._bot_msg(f"telemetryallow error: {e}")

    async def _bot_telemetrydeny(self, arg: str, perm: str):
        bits = _TELEM_PERM_MAP.get(perm)
        if bits is None:
            self._bot_msg(f"Unknown permission '{perm}'  —  use: data  sensors  location  all")
            return
        contact, err = self._resolve_contact_for_flags(arg)
        if err:
            self._bot_msg(err)
            return
        name = contact.get('adv_name', arg)
        pubkey = contact.get('public_key', '')
        new_flags = contact.get('flags', 0) & ~bits
        try:
            ev = await self.bridge.mc.commands.change_contact_flags(contact, new_flags)
            if ev and not ev.is_error():
                self.bridge.contacts[pubkey] = contact
                remaining = _fmt_telem_perms(new_flags)
                self._bot_msg(f"Telemetry {perm} denied: {name} [{pubkey[:12]}]  {remaining or 'no permissions'}")
            else:
                self._bot_msg(f"Failed to update flags for {name}")
        except Exception as e:
            self._bot_msg(f"telemetrydeny error: {e}")

    async def _bot_telemetrylist(self):
        mc = self.bridge.mc
        contacts = mc.contacts if mc else self.bridge.contacts
        telem_mask = _FLAG_TELEM_BASE | _FLAG_TELEM_LOCATION | _FLAG_TELEM_ENVIRONMENT
        with_perms = [(pubkey, c) for pubkey, c in contacts.items()
                      if c.get('flags', 0) & telem_mask]
        if not with_perms:
            self._bot_msg("No contacts have telemetry permissions set")
            return
        self._bot_msg(f"Contacts with telemetry permissions ({len(with_perms)}):")
        for pubkey, c in sorted(with_perms, key=lambda x: x[1].get('adv_name', '').lower()):
            name = sanitize_nick(c.get('adv_name', pubkey[:12]))
            perms = _fmt_telem_perms(c.get('flags', 0))
            self._bot_msg(f"  {name:<22} [{pubkey[:12]}]  {perms}")

    async def _bot_block(self, arg: str):
        # Resolve to get a clean nick and pubkey_prefix (best-effort).
        contact = self.bridge.contact_for_nick(arg)
        if contact:
            nick = sanitize_nick(contact.get('adv_name', arg))
            pubkey = contact.get('public_key', '') or contact.get('_pubkey_prefix', '')
            pubkey_prefix = pubkey[:12] if pubkey else ''
        else:
            nick = sanitize_nick(arg)
            pubkey_prefix = ''
        if self.bridge.block_add(nick, pubkey_prefix):
            detail = f" [{pubkey_prefix}]" if pubkey_prefix else ""
            self._bot_msg(f"Blocked: {nick}{detail}  (channel messages from this user will be dropped)")
        else:
            self._bot_msg(f"{nick} is already blocked")

    async def _bot_unblock(self, arg: str):
        nick = sanitize_nick(arg)
        if self.bridge.block_remove(nick):
            self._bot_msg(f"Unblocked: {nick}")
        else:
            self._bot_msg(f"Not in block list: {nick}  (use: blocklist)")

    async def _bot_blocklist(self):
        entries = self.bridge.blocklist_entries()
        if not entries:
            self._bot_msg("Block list is empty")
            return
        self._bot_msg(f"Blocked users ({len(entries)}):")
        for e in sorted(entries, key=lambda x: x['nick'].lower()):
            prefix = f" [{e['pubkey_prefix']}]" if e.get('pubkey_prefix') else ""
            self._bot_msg(f"  {e['nick']}{prefix}")

    async def _cmd_whois(self, rest: str):
        nick = rest.strip().split()[0] if rest.strip() else ''
        if not nick:
            self.numeric('431', ':No nickname given')
            return

        # Check IRC users first
        for client in self.bridge.irc_clients:
            if client.registered and client.nick and client.nick.lower() == nick.lower():
                self.numeric('311', nick, client.username or 'user', 'meshcore', '*',
                             f':{client.realname or nick}')
                self.numeric('312', nick, SERVER_NAME, ':MeshCore IRC Gateway')
                chans = ' '.join(f'@{ch}' for ch in client.joined_channels)
                if chans:
                    self.numeric('319', nick, f':{chans}')
                self.numeric('318', nick, ':End of /WHOIS list')
                return

        # Check MeshCore contacts and channel members
        contact = self.bridge.contact_for_nick(nick)
        member = self.bridge.channel_member_info(nick)

        # If not found by name, try resolving via the pubkey prefix stored in channel_members
        if not contact and member and member.get('host', 'mesh') != 'mesh':
            contact = self.bridge.contact_for_pubkey_prefix(member['host'])

        if not contact and not member:
            self.numeric('401', nick, ':No such nick/channel')
            return

        pubkey = (contact or {}).get('public_key', '')
        _prefix_from_contact = (contact or {}).get('_pubkey_prefix', '')
        pubkey_prefix = (pubkey[:12] if pubkey
                         else _prefix_from_contact
                         or (member.get('host', '?') if member else '?'))
        name = (contact or {}).get('adv_name', nick)
        lat = (contact or {}).get('adv_lat', 0.0)
        lon = (contact or {}).get('adv_lon', 0.0)
        advert_hops = self.bridge.advert_path_by_pubkey.get(pubkey, -1)
        if advert_hops < 0:
            advert_hops = (contact or {}).get('out_path_len', -1)
        last_advert = (contact or {}).get('last_advert', 0)
        # Merge in-memory min (current session) with persistent all-time min
        member_min = member.get('min_path_len', -1) if member else -1
        pubkey_key = (pubkey[:12] if pubkey
                      else _prefix_from_contact
                      or (member.get('host', '') if member else ''))
        stored_min = self.bridge.min_msg_hops_by_pubkey.get(pubkey_key, -1)
        last_path_len = (stored_min if member_min < 0
                         else stored_min if 0 <= stored_min < member_min
                         else member_min)
        last_seen = member.get('ts', 0) if member else 0
        ctype = (contact or {}).get('type', 0)

        self.numeric('311', nick, pubkey_prefix, 'meshcore', '*', f':{name}')
        self.numeric('312', nick, SERVER_NAME, ':MeshCore node')

        if pubkey:
            self.numeric('338', nick, f':Public key : {pubkey}')

        if last_path_len >= 0:
            msg_via = self.bridge.channel_msg_path_nodes.get(nick, [])
            msg_via_str = (' via ' + ' → '.join(msg_via)) if msg_via else ''
            self.numeric('338', nick, f':Msg hops   : {last_path_len}{msg_via_str}')

        if advert_hops >= 0:
            via = self.bridge.advert_path_nodes_by_pubkey.get(pubkey, [])
            via_str = (' via ' + ' → '.join(via)) if via else ''
            self.numeric('338', nick, f':Advert hops: {advert_hops}{via_str}')
        elif contact and not (contact or {}).get('_pubkey_prefix'):
            self.numeric('338', nick, ':Advert hops: unknown (flood path)')

        if lat or lon:
            self.numeric('338', nick, f':Position   : {lat:.6f}, {lon:.6f}')
            si = self.bridge.self_info
            if si and (si.get('adv_lat') or si.get('adv_lon')):
                dist = self.bridge.distance_km(si['adv_lat'], si['adv_lon'], lat, lon)
                self.numeric('338', nick, f':Distance   : {dist:.1f} km')

        web_cfg = self.bridge.config.get('webserver', {})
        if web_cfg and (lat or lon or advert_hops >= 0 or last_path_len >= 0):
            base = web_cfg.get('url', 'http://{}:{}'.format(
                self.bridge.config['irc']['host'], web_cfg.get('port', 8080)))
            self.numeric('338', nick, ':Map        : {}/map/nodes/{}'.format(
                base, urllib.parse.quote(nick)))

        if last_advert:
            dt = datetime.datetime.fromtimestamp(last_advert, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            self.numeric('338', nick, f':Last advert: {dt}')
        elif last_seen:
            dt = datetime.datetime.fromtimestamp(last_seen, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            self.numeric('338', nick, f':Last seen  : {dt}')

        if ctype:
            self.numeric('338', nick, f':Type       : {_NODE_TYPE_LABEL.get(ctype, str(ctype))}')

        self.numeric('318', nick, ':End of /WHOIS list')

    async def _cmd_who(self, rest: str):
        mask = rest.strip().split()[0] if rest.strip() else '*'
        for client in self.bridge.irc_clients:
            if client.registered:
                self.numeric('352', mask, client.username or 'user', 'meshcore',
                             SERVER_NAME, client.nick, 'H@', f':0 {client.realname or client.nick}')
        now = time.time()
        seen: set = set()
        ch_filter = mask.lower() if mask.startswith('#') else None
        for ch_lower, members in self.bridge.channel_members.items():
            if ch_filter and ch_lower != ch_filter:
                continue
            for nick, m in members.items():
                if nick in seen or now - m['ts'] >= self.bridge.member_ttl:
                    continue
                seen.add(nick)
                flags = ('G' if m.get('away', False) else 'H') + ('+' if m.get('voiced', False) else '')
                self.numeric('352', mask, 'mesh', m['host'], SERVER_NAME, nick, flags, f':0 {nick}')
        self.numeric('315', mask, ':End of /WHO list')

    async def _cmd_names(self, rest: str):
        channel = rest.strip().split()[0] if rest.strip() else ''
        if channel:
            await self._send_names(channel)
        else:
            for ch in self.bridge.all_irc_channels():
                await self._send_names(ch)

    async def _cmd_list(self):
        self.numeric('321', 'Channel :Users  Name')
        for ch in self.bridge.all_irc_channels():
            count = sum(1 for c in self.bridge.irc_clients
                        if c.registered and ch.lower() in c.joined_channels)
            topic = self.bridge.channel_topic(ch)
            self.numeric('322', f'{ch} {count} :{topic}')
        self.numeric('323', ':End of /LIST')

    async def _cmd_mode(self, rest: str):
        parts = rest.split()
        if not parts:
            return
        target = parts[0]
        if target.startswith('#'):
            self.numeric('324', target, '+')
            self.numeric('329', target, '0')
        elif self.nick and target.lower() == self.nick.lower():
            self.numeric('221', '+i')

    async def _cmd_topic(self, rest: str):
        parts = rest.split(' :', 1)
        channel = parts[0].strip()
        if len(parts) > 1:
            self.numeric('482', channel, ":You're not channel operator")
        else:
            topic = self.bridge.channel_topic(channel)
            if topic:
                self.numeric('332', channel, f':{topic}')
            else:
                self.numeric('331', channel, ':No topic is set')


def _generate_self_signed_cert(cert_path: str, key_path: str):
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.datetime.now(datetime.timezone.utc)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'meshcoreirc')])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=3650))
        .sign(key, hashes.SHA256())
    )
    Path(key_path).write_bytes(key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ))
    Path(cert_path).write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    logger.info("Self-signed TLS certificate written to %s / %s (valid 10 years)", cert_path, key_path)


class IRCServer:
    def __init__(self, bridge: Bridge):
        self.bridge = bridge

    async def run(self):
        irc_cfg = self.bridge.config['irc']
        host = irc_cfg['host']
        port = irc_cfg['port']
        ssl_port = irc_cfg.get('ssl_port')

        servers = []

        plain = await asyncio.start_server(self._handle_client, host, port)
        logger.info("IRC server listening on %s (plain)",
                    ', '.join(str(s.getsockname()) for s in plain.sockets))
        servers.append(plain)

        if ssl_port:
            ssl_ctx = self._build_ssl_context(irc_cfg)
            tls = await asyncio.start_server(self._handle_client, host, ssl_port, ssl=ssl_ctx)
            logger.info("IRC server listening on %s (TLS)",
                        ', '.join(str(s.getsockname()) for s in tls.sockets))
            servers.append(tls)

        async with contextlib.AsyncExitStack() as stack:
            for srv in servers:
                await stack.enter_async_context(srv)
            await asyncio.gather(*(srv.serve_forever() for srv in servers))

    @staticmethod
    def _build_ssl_context(irc_cfg: dict) -> ssl.SSLContext:
        cert = irc_cfg.get('ssl_cert', 'irc_cert.pem')
        key  = irc_cfg.get('ssl_key',  'irc_key.pem')
        if not Path(cert).exists() or not Path(key).exists():
            logger.info("Generating self-signed TLS certificate (%s / %s)", cert, key)
            _generate_self_signed_cert(cert, key)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile=cert, keyfile=key)
        return ctx

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        logger.info("IRC connection from %s", addr)
        client = IRCClient(reader, writer, self.bridge)
        await client.handle()
