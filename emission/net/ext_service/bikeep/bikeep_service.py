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
        list of station dicts with structure:
        {
            "station_id": "1",
            "name": "Downtown Station",
            "docks": [
                {
                    "dock_id": "1-1",
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
        response = requests.post(url, headers=headers, json=payload, timeout=10)
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
        response = requests.post(url, headers=headers, json=payload, timeout=10)
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

def book_dock(dock_id, timeout_at=None):
    """
    Book a dock via Bikeep API.

    Args:
        dock_id: Dock ID (e.g., "1-1")
        timeout_at: Optional booking expiration timestamp (ISO8601, UTC).

    Returns:
        dict with command/status info from Bikeep.

    Raises:
        RequestException: if Bikeep API fails
    """
    url = f"{BIKEEP_API_URL}/device/v1/devices/{dock_id}/commands"
    headers = _get_headers()
    payload = {"command": "book"}
    if timeout_at is not None:
        payload["timeout_at"] = timeout_at

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Booked dock {dock_id} via Bikeep")
        return data
    except Timeout:
        logger.error(f"Timeout booking dock {dock_id} via Bikeep")
        raise
    except ConnectionError:
        logger.error(f"Connection error booking dock {dock_id} via Bikeep")
        raise
    except RequestException as e:
        logger.error(f"Error booking dock {dock_id} via Bikeep: {e}")
        raise

def cancel_booking_dock(dock_id):
    """
    Cancel an active booking for a dock via Bikeep API.

    Args:
        dock_id: Dock ID (e.g., "1-1")

    Returns:
        dict with command/status info from Bikeep.

    Raises:
        RequestException: if Bikeep API fails
    """
    url = f"{BIKEEP_API_URL}/device/v1/devices/{dock_id}/commands"
    headers = _get_headers()
    payload = {"command": "cancel-booking"}

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Cancelled booking for dock {dock_id} via Bikeep")
        return data
    except Timeout:
        logger.error(f"Timeout cancelling booking for dock {dock_id} via Bikeep")
        raise
    except ConnectionError:
        logger.error(f"Connection error cancelling booking for dock {dock_id} via Bikeep")
        raise
    except RequestException as e:
        logger.error(f"Error cancelling booking for dock {dock_id} via Bikeep: {e}")
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
