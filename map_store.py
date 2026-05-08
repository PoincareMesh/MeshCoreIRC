import secrets
import time

_TTL = 24 * 3600
_store: dict[str, tuple[float, str, list]] = {}


def put(nodes: list, title: str = 'MeshCore Map') -> str:
    _prune()
    key = secrets.token_hex(4)
    _store[key] = (time.time(), title, nodes)
    return key


def get(key: str) -> tuple[str, list] | None:
    entry = _store.get(key)
    return (entry[1], entry[2]) if entry else None


def _prune():
    cutoff = time.time() - _TTL
    for k in [k for k, v in _store.items() if v[0] < cutoff]:
        del _store[k]
