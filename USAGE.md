# MeshCore IRC Gateway

An IRC server that bridges a MeshCore companion device to IRC clients.

## Configuration

Edit `config.toml` before first run:

```toml
[log]
file = "meshcoreirc.log"  # path to log file (omit to disable)
debug = false             # set to true for verbose debug logging

[meshcore]
tty = "/dev/ttyUSB0"   # serial device for your MeshCore companion
baudrate = 115200
max_msg_len = 200      # max message size in UTF-8 bytes before the bridge rejects it

[irc]
host = "127.0.0.1"     # address to bind the IRC server on
port = 6667
password = "changeme"  # set a strong password
member_timeout = 3600  # seconds before an inactive channel user is removed from the member list
voice_timeout = 600    # seconds a user keeps voice status (+) after their last message
mention_separators = ":,;"  # characters treated as separator after an @mention
# ssl_port = 6697      # enable TLS on this port (runs alongside plain port)
# ssl_cert = "irc_cert.pem"
# ssl_key  = "irc_key.pem"

[webserver]
port = 8080
bind = "0.0.0.0"
# url = "https://meshcore.example.com"  # set if behind a reverse proxy
nodes_refresh_intervals = [120, 300, 600]  # auto-refresh options for /map/nodes (seconds)
```

## Running

### First time (set up virtualenv)

```bash
cd /path/to/meshcoreirc
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 main.py
```

### Subsequent runs

```bash
cd /path/to/meshcoreirc
source .venv/bin/activate
python3 main.py
```

Stop with `Ctrl+C`.

## Connecting with an IRC client

The server requires a **server password** for authentication. Use the PASS method when connecting — not NickServ or SASL.

| Setting   | Value                              |
|-----------|------------------------------------|
| Server    | `127.0.0.1` (or your host)        |
| Port      | `6667`                             |
| Password  | value of `[irc] password` in config |
| Nick      | anything — overwritten with the MeshCore node name on connect |

### Example client settings (WeeChat)

```
/server add meshcore 127.0.0.1/6667 -password=changeme -nick=meshuser
/connect meshcore
```

With TLS (self-signed cert — accept the fingerprint when prompted):

```
/server add meshcore 127.0.0.1/6697 -password=changeme -nick=meshuser -ssl -ssl_verify=0
/connect meshcore
```

### Example client settings (irssi)

```
/network add meshcore
/server add -net meshcore -host 127.0.0.1 -port 6667 -pass changeme
/connect meshcore
```

With TLS:

```
/network add meshcore
/server add -net meshcore -host 127.0.0.1 -port 6697 -pass changeme -tls -tls_verify=no
/connect meshcore
```

## Multiple clients

You can connect as many IRC clients as you like simultaneously. All connected clients share the same view: they join the same channels, see the same messages, and share the same nick (derived from the MeshCore node name).

The username field (shown as `user@host` in IRC) is ignored by the server — only the password matters for authentication.

## Channels

Channels are named after the MeshCore channel names configured on your device. If slot 1 is named `bemesh`, it becomes `#bemesh` on IRC. Slots without a name fall back to `#mesh-<idx>`.

All channels are joined automatically on connect. The MOTD lists the active channels with their slot numbers and names.

## @mentions

`@mention` syntax is translated in both directions, anywhere in a message:

- **IRC → MeshCore**: `@Nick` (with or without a separator like `:` or `,`) is converted to MeshCore's `@[OriginalName]` format, looking up the original name from the nick registry. Works at the start or in the middle of a message.
- *_MeshCore → IRC**: `@[OriginalName]` is converted to the corresponding IRC nick (`@Nick`), so IRC client highlight rules fire correctly.

The separator characters recognised after a nick on the IRC side are configurable via `[irc] mention_separators` (default `":,;"`).

If a message is too long for MeshCore (exceeds `[meshcore] max_msg_len` bytes in UTF-8), it is **not sent** and a notice is shown in the channel or DM tab where it was typed:

```
Msg too long: 243/200
```

## Node advertisements

Live node discoveries and advertisements are delivered as **server notices** — they appear in your IRC client's server/status window, not in any channel. Each notice looks like:

```
Advert: ON1AFF-PC [e7872dc653]  pos=51.0940,4.5142  hops=2 via BE-DUF-SiSCD-01
```

To save a discovered node as a contact, send `addcontact` to the `_MeshCore` bot:

```
/msg _MeshCore addcontact ON1AFF-PC
```

## MeshCore bot commands

Send commands via private message to the `_MeshCore` bot:

```
/msg _MeshCore help
```

| Command                                   | Description                          |
|-------------------------------------------|--------------------------------------|
| `help`                                    | Show command list                    |
| `contacts <all\|repeater\|companion\|sensor\|room> [filter]` | List contacts saved on the companion |
| `discovered <all\|repeater\|...> [filter]`| List contacts seen this session but not yet saved |
| `nodeinfo`                                | Show our own node details            |
| `get`                                     | Show all node settings               |
| `get/set power [<dBm>]`                   | TX power                             |
| `get/set radio [<MHz> <kHz> <sf> <cr>]`   | Radio parameters                     |
| `get/set name [<name>]`                   | Node name                            |
| `get/set coords [<lat> <lon>]`            | GPS coordinates                      |
| `get/set autoadd [<on\|off>]`             | Auto-add contacts when received      |
| `get/set lockey [<on\|off>]`              | Share GPS in advertisements          |
| `get/set multiack [<0-3>]`               | Multi-ack count                      |
| `get/set telemetry [<base\|loc\|env> <0-3>]` | Telemetry reporting level         |
| `get/set af [<1-9>]`                      | Airtime Factor                       |
| `get/set pathmode [<1-4>]`               | Path hash size in bytes (1=small, 4=large) |
| `get tuning\|bat\|stats\|deviceinfo\|customs` | Read-only setting groups         |
| `login <name> [password]`                 | Login to a repeater (uses saved password if omitted) |
| `logout <name>`                           | Logout from a repeater               |
| `synctime <name>`                         | Sync a repeater's clock to the gateway's current time |
| `savepassword <name> <password>`          | Save a repeater's login password persistently |
| `deletepassword <name>`                   | Remove a saved repeater password     |
| `passwords`                               | List repeaters with a saved password |
| `status <name>`                           | Request repeater status              |
| `neighbours <name>`                       | Request repeater neighbours          |
| `expand [name]`                           | Fetch next batch of neighbours       |
| `refreshcontacts`                         | Refresh contact list from companion  |
| `zeroadvert`                              | Send our own self-advertisement (zero-hop) |
| `floodadvert`                             | Send our own self-advertisement (flood) |
| `addchannel <name>`                       | Join a MeshCore channel by name (picks first free slot 0-7) |
| `addchannel <idx> <name>`                 | Join a MeshCore channel at a specific slot index |
| `addcontact <nick\|pubkey>`               | Save a discovered contact to the companion (survives restart) |
| `removecontact <nick\|pubkey>`            | Remove a contact from the companion  |
| `renamecontact <nick\|pubkey> <new name>` | Rename a saved contact on the companion |
| `telemetry <nick>`                        | Request telemetry from a contact (battery, GPS, sensors) |
| `telemetryallow <nick> <data\|sensors\|location\|all>` | Grant a contact permission to retrieve our telemetry |
| `telemetrydeny <nick> <data\|sensors\|location\|all>`  | Revoke a contact's telemetry permission |
| `telemetrylist`                           | List all contacts with telemetry permissions set |
| `block <nick\|pubkey>`                    | Ignore channel messages from a MeshCore user (persistent) |
| `unblock <nick>`                          | Remove a user from the block list    |
| `blocklist`                               | Show the block list                  |

`get/set` commands: `get <name>` shows the current value; `set <name> <value>` changes it.

`addcontact` and `renamecontact` accept a nick name, a full 64-character hex public key, or a unique prefix of a public key.

Block list entries are stored in `blocklist.json` (configurable via `[irc] blocklist_file`) and survive restarts. Only channel messages are affected — direct messages still arrive.

## Telemetry

MeshCore supports three independent telemetry data types, each with its own permission bit stored in the contact record on the companion device:

| Type       | Contains                          |
|------------|-----------------------------------|
| `data`     | Battery voltage, uptime, stats    |
| `location` | GPS coordinates                   |
| `sensors`  | Environment sensors (temp, humidity, etc.) |

### Requesting telemetry from a contact

```
/msg _MeshCore telemetry ON1AFF-PC
```

The remote device controls what it will share. Even with permissions set, the remote node must also have its own telemetry mode configured (`set telemetry loc 1`, etc.) before it will include that data type.

### Granting contacts access to our telemetry

Telemetry permissions are stored as flag bits on the contact record in our companion device. Setting them allows that contact to retrieve our telemetry data via the MeshCore binary protocol:

```
/msg _MeshCore telemetryallow ON1AFF-PC all
/msg _MeshCore telemetryallow ON1AFF-PC data
/msg _MeshCore telemetryallow ON1AFF-PC sensors
/msg _MeshCore telemetrydeny  ON1AFF-PC location
/msg _MeshCore telemetrylist
```

Our device's telemetry sharing mode (`set telemetry <base|loc|env> <0-3>`) controls the global policy:
- `0` — never share (deny all)
- `1` — share only with contacts that have the matching permission bit set
- `2` — share with everyone

Setting a permission bit only takes effect when the relevant telemetry mode is `1` (allow-by-flags). The `contacts` listing shows permission tags (`[data]`, `[location]`, `[sensors]`) next to contacts that have any telemetry permission set.

## Direct messages

MeshCore contacts appear as IRC nicks. You can send them a DM directly:

```
/msg <ContactName> Hello from IRC
```

## Repeater commands

Opening a DM with a repeater contact gives access to repeater commands. Type the command directly in the chat tab (no `/msg _MeshCore` prefix needed):

| Command | Description |
|---------|-------------|
| `login [password]` | Authenticate with the repeater (uses saved password if omitted) |
| `logout` | End the session |
| `synctime` | Sync the repeater's clock to the gateway's current time |
| `telemetry` | Request telemetry from the repeater (battery, GPS, sensors) |
| `advert` | Send a flood advertisement from the repeater |
| `zeroadvert` | Send a zero-hop advertisement from the repeater |
| `status` | Request repeater status |
| `neighbours` | List the repeater's neighbours |
| `expand` | Fetch the next batch of neighbours |
| `cli <command>` | Send a raw CLI command to the repeater |
| `savepassword <password>` | Save this repeater's login password persistently |
| `deletepassword` | Remove the saved password for this repeater |

`cli` sends the command string over the MeshCore protocol and the repeater's response arrives as a DM in the same chat tab. Login may be required depending on the repeater's configuration.

## WHOIS

Use `/whois <nick>` on any MeshCore contact to see full details: public key, GPS position, OpenStreetMap link, distance from your node, and hop count.

## Web server

The built-in web server runs on port 8080 by default (configurable via `[webserver] port`). It provides map and path visualisations.

| URL | Description |
|-----|-------------|
| `/map/` | Index page with links to all map pages |
| `/map/contacts/<type>` | Map of saved contacts, filtered by type (`all`, `repeater`, `companion`, `sensor`, `room`) |
| `/map/discovered/<type>` | Map of nodes seen this session but not yet saved, filtered by type |
| `/map/nodes` | Index of all known nodes with advert/message hop counts, distances, and hash sizes |
| `/map/nodes/<name>` | Path visualisation for a specific node: advert path (gray dashed) and message path (orange dashed), with unknown-position nodes shown as white dashed circles at interpolated positions |
| `/map/heath/1b` | Heat map of all 1-byte hash paths |
| `/map/heath/2b` | Heat map of all 2-byte hash paths |
| `/map/heath/3b` | Heat map of all 3-byte hash paths |
| `/map/neighbours` | Index of all cached repeater neighbour sets |
| `/map/neighbours/<name>` | Neighbour map for a specific repeater |

Bot commands that return map URLs (e.g. `contacts`, `discovered`, `neighbours`) include a direct link to the relevant map page.

### Heath path heat maps

`/map/heath/1b`, `/map/heath/2b`, and `/map/heath/3b` overlay every stored path (advert and message) for the given hash size on a single map, showing the full chain from each remote node through its repeater hops to the gateway.

- **Circle size** is proportional to how many paths were forwarded by that node.
- **Line thickness** is proportional to how many packets traversed that specific hop segment, scaled between `heath_line_weight_min` and `heath_line_weight_max` (configurable in `config.toml`).
- **Clicking a line** highlights all end-to-end paths that passed through it in blue. Click the map background to clear the highlight.
- Nodes whose position is unknown are shown with a **white circle and dashed border** at an interpolated position between their known neighbours, matching the style used on `/map/nodes/<name>`.
- Nodes resolved via the meshcore.io registry are shown with a **gray dashed circle**.

Message path data is stored persistently across restarts in `nodes.json` and is included alongside advert path data on these maps.

Line thickness range is configurable:

```toml
[webserver]
heath_line_weight_min = 3.0   # thinnest line (1 packet)
heath_line_weight_max = 10.0  # thickest line (most packets)
```

If the server is behind a reverse proxy, set `[webserver] url` in `config.toml` so that map links in bot replies and `/whois` output use the correct public URL.

### /map/nodes filters and auto-refresh

The node index page has three independent filter bars:

- **Type** — filter by node type (repeater, companion, room, sensor)
- **Data** — `Has msg` shows only nodes for which message path data was observed
- **Hash** — filter by path hash size (1B, 2B, 4B …), matches advert or message hash

Filter state, sort column/direction, and the selected refresh interval are all remembered in browser `localStorage` and restored on the next visit or auto-refresh.

The **Refresh** dropdown (Off by default) reloads the page at the chosen interval. The available intervals are configurable via `[webserver] nodes_refresh_intervals` in `config.toml` (values in seconds):

```toml
nodes_refresh_intervals = [120, 300, 600]   # shown as 2m, 5m, 10m
```

### meshcore.io location lookup

The web server can resolve unknown `?hash` nodes on path maps by querying the public [meshcore.io](https://meshcore.io) node registry. Configure in `config.toml`:

```toml
[webserver]
meshcore_map_url   = "https://map.meshcore.io/api/v1/nodes"
meshcore_map_cache = "meshcore_map_cache.json"
meshcore_map_refresh_hours = 48   # keep ≥ 48 to avoid overloading meshcore.io
```

The default refresh interval is **48 hours**. Please do not lower this significantly.
