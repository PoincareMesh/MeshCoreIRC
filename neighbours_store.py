import json
import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_dir = Path('neighbours')


def set_dir(path: str):
    global _dir
    _dir = Path(path)


def save(key: str, repeater_name: str, nodes: list,
         repeater_lat: float = 0.0, repeater_lon: float = 0.0):
    _dir.mkdir(exist_ok=True)
    path = _dir / f"{key}.json"
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump({
                'timestamp': time.time(),
                'repeater_name': repeater_name,
                'repeater_lat': repeater_lat,
                'repeater_lon': repeater_lon,
                'nodes': nodes,
            }, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error("Failed to save neighbours for %s: %s", key, e)


def load(key: str) -> dict | None:
    path = _dir / f"{key}.json"
    if not path.exists():
        return None
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error("Failed to load neighbours for %s: %s", key, e)
        return None


def list_all() -> list[dict]:
    """Return [{key, repeater_name, timestamp}] for all saved neighbour files."""
    if not _dir.exists():
        return []
    result = []
    for path in sorted(_dir.glob('*.json')):
        try:
            with open(path, encoding='utf-8') as f:
                data = json.load(f)
            result.append({
                'key': path.stem,
                'repeater_name': data.get('repeater_name', path.stem),
                'timestamp': data.get('timestamp', 0),
            })
        except Exception:
            pass
    return result
