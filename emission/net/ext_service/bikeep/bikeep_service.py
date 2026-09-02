import logging
import os
import json
import time
import requests
from requests.exceptions import Timeout, ConnectionError, RequestException

BIKEEP_API_URL = "https://services.bikeep.com"
BIKEEP_AUTH_URL = "https://auth.bikeep.com/oauth2/token"

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

_TOKEN_CACHE = {
    "access_token": None,
    "expires_at": 0,
}

def _get_api_credentials():
    """Load Bikeep credentials and URLs from environment or config file."""
    credentials = {
        "BIKEEP_CLIENT_ID": os.environ.get("BIKEEP_CLIENT_ID"),
        "BIKEEP_CLIENT_SECRET": os.environ.get("BIKEEP_CLIENT_SECRET"),
    }

    # Try config file
    try:
        config_path = "conf/net/ext_service/bikeep.json"
        with open(config_path) as f:
            config = json.load(f)
            for key in credentials:
                if not credentials[key]:
                    credentials[key] = config.get(key)
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        logger.warning(f"Could not load Bikeep config from file: {e}")

    if not bool(credentials["BIKEEP_CLIENT_ID"] and credentials["BIKEEP_CLIENT_SECRET"]):
        raise ValueError(
            "Bikeep credentials not set. Configure BIKEEP_CLIENT_ID + BIKEEP_CLIENT_SECRET."
        )

    return credentials

def _get_oauth_access_token(credentials):
    """Get (and cache) OAuth2 access token using client_credentials flow."""
    now = time.time()
    if _TOKEN_CACHE["access_token"] and now < _TOKEN_CACHE["expires_at"]:
        return _TOKEN_CACHE["access_token"]

    payload = {
        "grant_type": "client_credentials",
        "client_id": credentials["BIKEEP_CLIENT_ID"],
        "client_secret": credentials["BIKEEP_CLIENT_SECRET"],
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(
        BIKEEP_AUTH_URL,
        data=payload,
        headers=headers,
        timeout=10,
    )
    response.raise_for_status()

    token_response = response.json()
    access_token = token_response.get("access_token")
    expires_in = int(token_response.get("expires_in", 3600))

    if not access_token:
        raise ValueError("Bikeep token response missing access_token")

    # Refresh one minute early to avoid expired token issues
    _TOKEN_CACHE["access_token"] = access_token
    _TOKEN_CACHE["expires_at"] = now + max(0, expires_in - 60)
    return access_token

def _get_headers():
    """Get request headers with Bikeep auth."""
    credentials = _get_api_credentials()
    token = _get_oauth_access_token(credentials)

    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

def get_locations():
    """
    Get all locations, docks, and their lock states from Bikeep.
    
    Returns:
        list of location dicts with structure:
        {
            "id": "abc123",
            "name": "Downtown Station",
            "latitude": 37.7749,
            "longitude": -122.4194,
            "docks": [
                {
                    "dock_id": "abc123",  # dock_id == location_id
                    "lock_state": "locked" | "unlocked",
                    "bike_id": None,  # Bikeep doesn't track this; OpenPATH does
                }
            ]
        }
    
    Raises:
        RequestException: if Bikeep API fails
    """
    url = f"{BIKEEP_API_URL}/location/v1/locations"
    headers = _get_headers()
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json().get('data', [])
        logger.debug(f"Retrieved {len(data)} locations from Bikeep")
        return data
    except Timeout:
        logger.error(f"Timeout retrieving locations from Bikeep ({url})")
        raise
    except ConnectionError:
        logger.error(f"Connection error retrieving locations from Bikeep ({url})")
        raise
    except RequestException as e:
        logger.error(f"Error retrieving locations from Bikeep: {e}")
        raise

def get_devices(location_id):
    """
    Get all devices (docks/lockers) at a single Bikeep location.

    Returns:
        list of device dicts, e.g.:
        {
            "id": "f1e2d3c4-...",
            "type": "LOCKER" | "BIKE_DOCK" | ...,
            "alias": "1",
            "code": "222222",  # human-readable code printed/scanned at the device
            "state": {"value": "LOCKED" | "UNLOCKED" | ..., "changed_at": "..."},
        }

    Raises:
        RequestException: if Bikeep API fails
    """
    url = f"{BIKEEP_API_URL}/device/v1/locations/{location_id}/devices"
    headers = _get_headers()

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json().get('data', [])
        logger.debug(f"Retrieved {len(data)} devices for location {location_id} from Bikeep")
        return data
    except Timeout:
        logger.error(f"Timeout retrieving devices for location {location_id} from Bikeep")
        raise
    except ConnectionError:
        logger.error(f"Connection error retrieving devices for location {location_id} from Bikeep")
        raise
    except RequestException as e:
        logger.error(f"Error retrieving devices for location {location_id} from Bikeep: {e}")
        raise


_all_devices_cache = []
_all_devices_cache_expiry_ts = 0

def get_locations_and_all_devices():
    """
    Get all devices for all Bikeep locations.

    Returns:
        tuple of (locations, all_devices)
        locations: list of location dicts
        all_devices: list of all device dicts across all locations
    """
    locations = get_locations()
    all_devices = []
    for location in locations:
        try:
            location_id = location.get('id')
            if not location_id:
                continue
            all_devices.extend(get_devices(location_id))
        except Exception as e:
            logger.warning(f"Could not fetch devices for location {location_id}: {e}")

    global _all_devices_cache, _all_devices_cache_expiry_ts
    _all_devices_cache = all_devices
    _all_devices_cache_expiry_ts = time.time() + 600  # 10 minutes

    return locations, all_devices


def get_device_id_for_code(code):
    """
    Resolve a scanned/printed device "code" to the actual Bikeep device "id"
    needed for lock/unlock commands. Returns None if no device matches.
    """
    if _all_devices_cache and time.time() < _all_devices_cache_expiry_ts:
        all_devices = _all_devices_cache
    else:
        _, all_devices = get_locations_and_all_devices()

    for device in all_devices:
        if device.get('code') == code:
            return device.get('id')
    return None


def get_location(device_id):
    """
    Get a location object for a Bikeep device.

    The input ID is a Bikeep device ID. We first retrieve the device from
    /device/v1/devices/{device_id}, then follow the location reference in the
    device payload to fetch the location object.

    Returns a dict with at least: id, name, latitude, longitude.
    Raises RequestException if the API call fails.
    """
    device_url = f"{BIKEEP_API_URL}/device/v1/devices/{device_id}"
    headers = _get_headers()

    try:
        device_response = requests.get(device_url, headers=headers, timeout=10)
        device_response.raise_for_status()
        device_data = device_response.json()

        location_ref = device_data.get("location", {}).get("uri")
        if location_ref is None:
            raise ValueError(400, f"Device {device_id} response missing location reference")

        location_response = requests.get(location_ref, headers=headers, timeout=10)
        location_response.raise_for_status()
        data = location_response.json()
        logger.debug(f"Retrieved location for device {device_id} from Bikeep")
        return data
    except Timeout:
        logger.error(f"Timeout retrieving location for device {device_id} from Bikeep")
        raise
    except ConnectionError:
        logger.error(f"Connection error retrieving location for device {device_id} from Bikeep")
        raise
    except RequestException as e:
        logger.error(f"Error retrieving location for device {device_id} from Bikeep: {e}")
        raise

def lock_dock(dock_id):
    """
    Lock a dock via Bikeep API.
    
    Args:
        dock_id: Dock ID (e.g., "1-1")
    
    Returns:
        dict with status info: {"status": "locked"}
    
    Raises:
        RequestException: if Bikeep API fails
    """
    url = f"{BIKEEP_API_URL}/device/v1/devices/{dock_id}/commands"
    headers = _get_headers()
    payload = {"command": "lock"}
    
    try:
        logger.debug(f"About to lock dock {dock_id}, {headers=}, {payload=}")
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        logger.debug(f"Response from locking dock {dock_id}: {response}")
        if response.status_code != 200:
            logger.error(f"Response from locking dock {dock_id}: {response}")
            logger.error(f"Failed to lock dock {dock_id}, status code: {response.status_code}, response: {response.error_code=}, {response.error_message=}")
        response.raise_for_status()
        data = response.json()
        logger.info(f"Locked dock {dock_id} via Bikeep")
        return data
    except Timeout:
        logger.error(f"Timeout locking dock {dock_id} via Bikeep")
        raise
    except ConnectionError:
        logger.error(f"Connection error locking dock {dock_id} via Bikeep")
        raise
    except RequestException as e:
        logger.error(f"Error locking dock {dock_id} via Bikeep: {e}")
        raise

def unlock_dock(dock_id):
    """
    Unlock a dock via Bikeep API.
    
    Args:
        dock_id: Dock ID (e.g., "1-1")
    
    Returns:
        dict with status info: {"status": "unlocked", "unlock_code": "..."}
    
    Raises:
        RequestException: if Bikeep API fails
    """
    url = f"{BIKEEP_API_URL}/device/v1/devices/{dock_id}/commands"
    headers = _get_headers()
    payload = {"command": "unlock"}
    
    try:
        logger.debug(f"About to unlock dock {dock_id}, {headers=}, {payload=}")
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        logger.debug(f"Response from unlocking dock {dock_id}: {response.text}")
        if response.status_code != 200:
            response_json = response.json()
            logger.error(f"Response from unlocking dock {dock_id}: {response_json}")
            logger.error(f"Failed to unlock dock {dock_id}, status code: {response.status_code}, response: {response_json.get('error_code')=}, {response_json.get('error_message')=}")
        response.raise_for_status()
        data = response.json()
        logger.info(f"Unlocked dock {dock_id} via Bikeep")
        return data
    except Timeout:
        logger.error(f"Timeout unlocking dock {dock_id} via Bikeep")
        raise
    except ConnectionError:
        logger.error(f"Connection error unlocking dock {dock_id} via Bikeep")
        raise
    except RequestException as e:
        logger.error(f"Error unlocking dock {dock_id} via Bikeep: {e}")
        raise

def take_photo(device_id):
    """
    Trigger a device photo capture via Bikeep API.

    Args:
        device_id: Device ID (typically a GUARD with camera capability).

    Returns:
        dict with command/status info from Bikeep.

    Raises:
        RequestException: if Bikeep API fails
    """
    url = f"{BIKEEP_API_URL}/device/v1/devices/{device_id}/commands"
    headers = _get_headers()
    payload = {"command": "take-photo"}

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Triggered photo capture for device {device_id} via Bikeep")
        return data
    except Timeout:
        logger.error(f"Timeout triggering photo capture for device {device_id} via Bikeep")
        raise
    except ConnectionError:
        logger.error(f"Connection error triggering photo capture for device {device_id} via Bikeep")
        raise
    except RequestException as e:
        logger.error(f"Error triggering photo capture for device {device_id} via Bikeep: {e}")
        raise
