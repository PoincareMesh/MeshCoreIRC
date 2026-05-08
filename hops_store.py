import json
import logging
import os

logger = logging.getLogger(__name__)


def load(path: str) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        logger.warning("Failed to load hops cache %s: %s", path, e)
        return {}


def save(path: str, data: dict):
    tmp = path + '.tmp'
    try:
        with open(tmp, 'w') as f:
            json.dump(data, f)
        os.replace(tmp, path)
    except Exception as e:
        logger.warning("Failed to save hops cache %s: %s", path, e)
