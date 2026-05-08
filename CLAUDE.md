# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the gateway

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python3 main.py
```

Requires `config.toml` (copy from `config.toml.example`). The three required keys are `[meshcore] tty`, `[irc] password`, and `[irc] port`.

There are no automated tests and no lint/build step. The only dependency beyond stdlib is `meshcore` (PyPI) and `tomli` on Python < 3.11.

**Always update `config.toml.example`** whenever new config keys are added. Never add protocol-constrained values (IRC numerics, MeshCore packet fields) to the example.

## Architecture overview

The program is a single asyncio process with three concurrent tasks launched from `main.py`:

| Task | File | Role |
|------|------|------|
| `MeshCoreHandler.run()` | `meshcore_handler.py` | Connects to MeshCore over serial; subscribes to events |
| `IRCServer.run()` | `irc.py` | Listens for IRC clients; handles all IRC protocol and bot commands |
| `run_web()` | `web_server.py` | Serves HTTP map pages (pure asyncio, no framework) |

All three share a single `Bridge` instance (`bridge.py`) which is the central state store.

### Bridge — shared state

`Bridge` holds everything that crosses subsystem boundaries:

- `contacts` — pubkey → contact dict, from the MeshCore device
- `channels` — slot index → channel display name
- `channel_members` — `channel_lower` → `{nick: last_seen}` (presence/voice tracking)
- `advert_path_by_pubkey`, `advert_path_nodes_by_pubkey`, `advert_last_ts_by_pubkey` — best incoming advert path data
- `min_msg_hops_by_pubkey` — keyed by 12-char pubkey prefix
- `channel_msg_path_nodes` — nick → list of resolved hop names (most recent message path)
- `self_info` — our own node info from the MeshCore device
- `node_cache` — `NodeCache` instance (set by `main.py`)
- `meshcore_map` — `MeshCoreMapCache` instance (set by `main.py`)
- `_name_to_nick` / `_nick_to_name` — bidirectional nick registry for collision-free sanitization

`sanitize_nick()` in `bridge.py` is the canonical nick cleaner (used everywhere — keep it in sync with `_sanitize_nick()` in `node_cache.py`).

### NodeCache — persistent storage (`node_cache.py`)

Unified on-disk cache written to `nodes.json`. Three key schemes:

| Prefix | Meaning |
|--------|---------|
| `<64-char hex>` | Full-pubkey entry — from advert, has lat/lon/type/path data |
| `p:<12-char hex>` | Prefix-only entry — channel-message node with no advert received |
| `n:<nick>` | Name-only entry — `host='mesh'` channel messages with no pubkey prefix |

On startup, `main.py` calls `bridge.load_hops_data(cache.hops_data())` then `bridge.load_msg_paths_from_cache()` to restore in-memory state from disk. When a full-pubkey advert arrives, `NodeCache.update()` absorbs any matching `p:` and `n:` entries into the full entry.

### MeshCoreHandler (`meshcore_handler.py`)

Subscribes to four MeshCore events:
- `CONTACT_MSG_RECV` → routes DM to all connected IRC clients
- `CHANNEL_MSG_RECV` → broadcasts to the matching IRC channel; resolves path hashes to names; persists path data via `node_cache`
- `ADVERTISEMENT` → async-fetches the advert path then announces via `broadcast_system()`
- `NEW_CONTACT` → merges into `bridge.contacts` and fetches path

Channel messages from old firmware have no `pubkey_prefix` (host is `'mesh'`). These take the `by_nick` persistence path (`update_msg_path_by_nick`, `update_channel_node_by_nick`). Messages with a `pubkey_prefix` use the prefix-keyed path.

### IRCServer / IRCClient (`irc.py`)

`IRCServer` accepts connections and spawns an `IRCClient` per connection. All clients share the same nick (the MeshCore node name), channels, and message view.

`IRCClient._handle_bot()` is a large `if/elif` chain dispatching `_MeshCore` bot commands. Each `_bot_*` method uses `self._bot_notice()` / `self._bot_msg()` for replies.

`@mention` translation:
- IRC → MeshCore: `_irc_to_mc_mention()` in `irc.py` (uses `bridge.assign_contact_nick()` reverse lookup)
- MeshCore → IRC: `_mc_to_irc_mention()` in `meshcore_handler.py`

### Web server (`web_server.py`)

Pure asyncio HTTP, no framework. All HTML is inline string templates (`_HTML_TEMPLATE`, `_PATH_TEMPLATE`, `_HEATH_TEMPLATE`, `_NODES_INDEX_*`). Templates use Python `str.format()` with `{{`/`}}` for literal JS braces.

**Critical editing note:** The template strings contain non-breaking spaces (`\xa0`). The `Edit` tool fails to match these. Use a Python script with `src.replace(OLD, NEW)` for any edits to template sections.

Key web routes and their render functions:

| Route | Builder |
|-------|---------|
| `/map/nodes` | `_nodes_index_entries()` → `_render_nodes_index()` |
| `/map/nodes/<name>` | `_node_path_data()` → `_render_node_path()` |
| `/map/heath/<Xb>` | `_heath_data()` → `_render_heath()` |
| `/map/contacts/<type>` | `bridge.contacts_map_nodes()` → `_render()` |
| `/map/neighbours/<name>` | `neighbours_store.load()` → `_render()` |

#### Path visualisation JS (`_PATH_TEMPLATE`)

The per-node path page embeds two JS arrays: `ADVERT_CHAIN` and `MSG_CHAIN`. Each element is `{name, lat, lon, role, src, pub_prefix}` where `role` is `'self'`/`'hop'`/`'target'` and `src` is `'local'`/`'remote'`/`'interpolated'`.

Rendering order (order matters for z-index):
1. `fillUnknown(ADVERT_CHAIN)` then `fillUnknown(MSG_CHAIN)` — fills `lat`/`lon`/`src='interpolated'` for unknown-position nodes in-place; no markers drawn here
2. `drawPath()` twice — draws polylines and distance labels (skips interpolated-to-interpolated segments)
3. Unified marker loop — draws all markers on top of lines; deduplicates by name; dashed circle for `src='interpolated'`

`fillUnknown` unknown-run placement (start-of-chain unknowns with `nextNode` but no `prevNode`):
- Requires `chainEnd !== nextNode` (two distinct reference points to establish direction)
- Uses `(runLen - k)` multiplier so the farthest node (target, k=0) is placed farthest from `nextNode` and the segment lengths are equal
- Adds a perpendicular offset (`0.012°`) so the unknown tail doesn't overlap the main path line

`_resolve_path_locs()` does two-pass closest-neighbor resolution of `?hash` nodes via the meshcore.io cache, then a stability pass that reverts placements farther than `meshcore_map_max_hop_km` from their neighbours.

`_build_chain()` constructs the chain as `[target, via..., self]` — target is always index 0, gateway (self) is always last.

### MeshCoreMapCache (`meshcore_map.py`)

Fetches and caches the meshcore.io node registry. Provides three lookup methods used by `web_server.py`:
- `lookup_by_name(name)` — exact sanitized-nick match
- `lookup_by_prefix_unique(prefix)` — returns a match only if exactly one node has that prefix
- `lookup_by_prefix_closest(prefix, ref_lat, ref_lon)` — returns the geographically closest match within `max_hop_km`

Default refresh is 48 h. Do not lower this without good reason.
