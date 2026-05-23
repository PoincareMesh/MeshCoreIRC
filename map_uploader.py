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

Signing is delegated to the companion device via mc.commands.sign(). The
companion stores its Ed25519 key in RFC 8032 *expanded* form (clamped scalar
|| prefix); naïvely feeding that to python-cryptography's
Ed25519PrivateKey.from_private_bytes — which expects a seed — yields a
totally different keypair and the API rejects the signature. Signing on the
device sidesteps that whole problem.
"""
import asyncio
import hashlib
import json
import logging
import time
import urllib.request
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_URL = 'https://map.meshcore.io/api/v1/uploader/node'
# Match the reference uploader: skip republishing the same node within an hour.
MIN_REUPLOAD_INTERVAL = 3600


def _radio_params(bridge) -> dict:
    """Return the radio settings as the map API expects them: freq in MHz, bw in kHz.

    The python-meshcore lib already reports these in MHz / kHz (see how nodeinfo
    prints them), so unlike the JS reference uploader we do *not* divide by 1000.
    """
    si = bridge.self_info or {}
    return {
        'freq': si.get('radio_freq', 0),
        'bw':   si.get('radio_bw', 0),
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
    mc = bridge.mc
    if mc is None:
        return False, "MeshCore not connected"

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
        sign_ev = await mc.commands.sign(digest)
    except Exception as e:
        return False, f"sign command exception: {e}"
    if sign_ev is None or sign_ev.is_error():
        reason = sign_ev.payload.get('reason', '?') if sign_ev else 'no response'
        return False, f"device sign failed: {reason}"
    sig_bytes = sign_ev.payload.get('signature') if isinstance(sign_ev.payload, dict) else None
    if not sig_bytes or len(sig_bytes) != 64:
        return False, f"device returned unexpected signature: {sign_ev.payload!r}"

    request_body = json.dumps({
        'data': data_json,
        'signature': bytes(sig_bytes).hex(),
        'publicKey': own_pubkey,
    }).encode('utf-8')

    status, body = await asyncio.to_thread(_post, url, request_body)
    body_short = body.strip()[:200]
    # The API sometimes returns HTTP 200 with {"error": "...", "code": "..."} —
    # treat any body containing an "error" key as a failure.
    api_error = None
    try:
        parsed = json.loads(body)
        if isinstance(parsed, dict) and parsed.get('error'):
            api_error = f"{parsed.get('code', 'ERR')}: {parsed['error']}"
    except (ValueError, TypeError):
        pass
    if 200 <= status < 300 and api_error is None:
        bridge.last_map_upload_ts[pubkey] = int(time.time())
        return True, body_short
    if api_error:
        return False, f"HTTP {status}: {api_error}"
    return False, f"HTTP {status}: {body_short}"
