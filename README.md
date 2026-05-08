# MeshCore IRC Gateway

An IRC server that bridges a [MeshCore](https://meshcore.co.uk) LoRa mesh radio companion device to IRC clients.

Connect any standard IRC client to the gateway and send/receive MeshCore channel messages and direct messages, browse node maps, and manage contacts — all from your IRC client.

## Features

- Channel messages and direct messages bridged in both directions
- Live node advertisements shown as server notices
- Built-in web server with interactive Leaflet maps (contact map, path visualisation, neighbour maps)
- Bot commands for contact management, node settings, and repeater control
- `@mention` translation between IRC nick style and MeshCore `@[Name]` format
- Configurable message length guard (rejects oversized messages before they reach the radio)

## Getting started

See [USAGE.md](USAGE.md) for installation, configuration, and a full command reference.

---

> **Disclaimer:** The source code in this repository was written by [Claude](https://claude.ai) (Anthropic's AI assistant) in collaboration with the repository owner. It is provided as-is, without warranty of any kind.
