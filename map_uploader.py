"""Upload a contact's raw advert to the meshcore.io map registry.

The map uploader API at https://map.meshcore.io/api/v1/uploader/node accepts a
signed JSON payload containing the raw advert packet (hex) of the node to be
published. The advert itself carries the node's signed name/type/lat/lon, so
the map can trust the contact's data without us re-signing it. The outer
request is signed with *our own* node's private key so the API can rate-limit
and de-duplicate per uploader.

Request shape (matches map.meshcore.io-uploader/index.mjs):
    {
        "data":      "<json string>",
        "signature": "<128-hex chars — Ed25519 signature over SHA-256(data)>",
        "publicKey": "<our 64-hex pubkey>"
    }
where the inner JSON is:
    {
        "params": {"freq": MHz, "cr": int, "sf": int, "bw": kHz},
        "links":  ["meshcore://<raw_advert_hex>"]
    }
"""
import asyncio
import hashlib
import json
import logging
import time
import urllib.request
from typing import Optional

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

logger = logging.getLogger(__name__)

DEFAULT_URL = 'https://map.meshcore.io/api/v1/uploader/node'
# Match the reference uploader: skip republishing the same node within an hour.
MIN_REUPLOAD_INTERVAL = 3600


async def _get_private_key_seed(bridge) -> Optional[bytes]:
    """Fetch and cache the 32-byte Ed25519 seed from the companion device.

    The MeshCore companion returns a 64-byte secret (32 seed + 32 pubkey), but
    python-cryptography only needs the 32-byte seed half.
    """
    if bridge._mc_private_seed is not None:
        return bridge._mc_private_seed
    mc = bridge.mc
    if not mc:
        return None
    ev = await mc.commands.export_private_key()
    if ev is None or ev.is_error():
        reason = ev.payload.get('reason', '?') if ev else 'no response'
        logger.warning("export_private_key failed: %s", reason)
        return None
    sk = ev.payload.get('private_key')
    if not sk or len(sk) < 32:
        logger.warning("export_private_key returned unexpected payload: %r", ev.payload)
        return None
    bridge._mc_private_seed = bytes(sk[:32])
    return bridge._mc_private_seed


def _radio_params(bridge) -> dict:
    si = bridge.self_info or {}
    return {
        'freq': si.get('radio_freq', 0) / 1000.0,
        'bw':   si.get('radio_bw', 0) / 1000.0,
        'sf':   si.get('radio_sf', 0),
        'cr':   si.get('radio_cr', 0),
    }


def _post(url: str, body: bytes, timeout: float = 15.0) -> tuple[int, str]:
    req = urllib.request.Request(
        url, data=body, method='POST',
        headers={'Content-Type': 'application/json'},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode('utf-8', 'replace')
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8', 'replace') if e.fp else str(e)
    except Exception as e:
        return 0, f"{type(e).__name__}: {e}"


async def upload_node(bridge, pubkey: str, raw_advert_hex: str,
                      url: Optional[str] = None) -> tuple[bool, str]:
    """Sign and POST a single node advert to the map registry.

    Returns (ok, message). On success, message is the API response body.
    """
    if url is None:
        url = bridge.config.get('meshcore_map', {}).get('url', DEFAULT_URL)
    seed = await _get_private_key_seed(bridge)
    if seed is None:
        return False, "could not fetch our private key from the companion device"

    own_pubkey = (bridge.self_info or {}).get('public_key', '')
    if not own_pubkey:
        return False, "our own pubkey not known yet (still connecting?)"

    data_obj = {
        'params': _radio_params(bridge),
        'links':  [f'meshcore://{raw_advert_hex}'],
    }
    data_json = json.dumps(data_obj, separators=(',', ':'))
    digest = hashlib.sha256(data_json.encode('utf-8')).digest()

    try:
        signature = Ed25519PrivateKey.from_private_bytes(seed).sign(digest)
    except Exception as e:
        return False, f"signing failed: {e}"

    request_body = json.dumps({
        'data': data_json,
        'signature': signature.hex(),
        'publicKey': own_pubkey,
    }).encode('utf-8')

    status, body = await asyncio.to_thread(_post, url, request_body)
    if 200 <= status < 300:
        bridge.last_map_upload_ts[pubkey] = int(time.time())
        return True, body
    return False, f"HTTP {status}: {body[:200]}"
