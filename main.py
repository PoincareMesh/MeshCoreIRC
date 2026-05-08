#!/usr/bin/env python3
import asyncio
import logging
import sys
from pathlib import Path

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        print("Python 3.11+ required, or install tomli:  pip install tomli", file=sys.stderr)
        sys.exit(1)

from bridge import Bridge
from node_cache import NodeCache
from irc import IRCServer
from meshcore_handler import MeshCoreHandler
from meshcore_map import MeshCoreMapCache, run_refresh_loop
from web_server import run as run_web

def setup_logging(config: dict):
    log_cfg = config.get('log', {})
    level = logging.DEBUG if log_cfg.get('debug') else logging.INFO
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    root.addHandler(ch)

    log_file = log_cfg.get('file')
    if log_file:
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setFormatter(formatter)
        root.addHandler(fh)

    log_filter = log_cfg.get('filter', '').lower()
    if log_filter == 'irc':
        logging.getLogger('meshcore').setLevel(logging.CRITICAL)
        logging.getLogger('meshcore_handler').setLevel(logging.CRITICAL)
    elif log_filter == 'meshcore':
        logging.getLogger('irc').setLevel(logging.CRITICAL)


logger = logging.getLogger(__name__)


def load_config(path: Path) -> dict:
    with open(path, 'rb') as f:
        return tomllib.load(f)


async def main():
    config_path = Path('config.toml')
    if not config_path.exists():
        logger.error("config.toml not found — copy and edit the example config")
        sys.exit(1)

    config = load_config(config_path)
    setup_logging(config)

    required = [('meshcore', 'tty'), ('irc', 'password'), ('irc', 'port')]
    for section, key in required:
        if section not in config or key not in config[section]:
            logger.error("Missing config key: [%s] %s", section, key)
            sys.exit(1)

    bridge = Bridge(config)
    bridge.load_blocklist(config.get('irc', {}).get('blocklist_file', 'blocklist.json'))
    bridge.load_passwords(config.get('irc', {}).get('passwords_file', 'repeater_passwords.json'))
    cache_cfg = config.get('cache', {})
    bridge.node_cache = NodeCache(
        cache_cfg.get('file', 'nodes.json'),
        cache_cfg.get('max_age_hours', 336),
    )
    bridge.load_hops_data(bridge.node_cache.hops_data())
    bridge.load_msg_paths_from_cache()

    web_map_cfg = config.get('webserver', {})
    bridge.meshcore_map = MeshCoreMapCache(
        cache_file=web_map_cfg.get('meshcore_map_cache', 'meshcore_map_cache.json'),
        refresh_hours=float(web_map_cfg.get('meshcore_map_refresh_hours', 48)),
        url=web_map_cfg.get('meshcore_map_url', 'https://map.meshcore.io/api/v1/nodes'),
    )

    logger.info(
        "Starting MeshCore IRC gateway  tty=%s  irc=%s:%d",
        config['meshcore']['tty'],
        config['irc']['host'],
        config['irc']['port'],
    )

    tasks = [
        MeshCoreHandler(bridge).run(),
        IRCServer(bridge).run(),
        run_refresh_loop(bridge.meshcore_map),
    ]
    web_cfg = config.get('webserver', {})
    if web_cfg:
        bind = web_cfg.get('bind', '0.0.0.0')
        port = web_cfg.get('port', 8080)
        tasks.append(run_web(bind, port, bridge))
        public = web_cfg.get('url', f"http://{config['irc']['host']}:{port}")
        logger.info("Map web server: %s/map", public)

    await asyncio.gather(*tasks)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down")
