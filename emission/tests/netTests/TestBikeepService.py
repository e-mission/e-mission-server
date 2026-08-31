"""
Tests for the Bikeep service.

Bikeep HTTP calls are mocked so this suite runs reliably in CI without
external API access or credentials.
"""

# Standard imports
from builtins import *
import unittest
import os
import time
import logging
from unittest import mock

# Module under test
import emission.net.ext_service.bikeep.bikeep_service as bikeep

logger = logging.getLogger(__name__)


TEST_DEVICE_ID = os.environ.get("BIKEEP_TEST_DEVICE_ID", "mock-dock-1")
TEST_CAMERA_DEVICE_ID = os.environ.get("BIKEEP_TEST_CAMERA_DEVICE_ID", "mock-camera-1")


class _MockResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = str(payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


class TestBikeepServiceMocked(unittest.TestCase):
    """
    Fast Bikeep service tests that mock external Bikeep calls.
    """

    def setUp(self):
        bikeep._TOKEN_CACHE["access_token"] = "mock-token"
        bikeep._TOKEN_CACHE["expires_at"] = time.time() + 3600

        self._credentials_patcher = mock.patch.object(
            bikeep,
            "_get_api_credentials",
            return_value={
                "BIKEEP_CLIENT_ID": "mock-client-id",
                "BIKEEP_CLIENT_SECRET": "mock-client-secret",
            },
        )
        self._mock_get_patcher = mock.patch.object(bikeep.requests, "get")
        self._mock_post_patcher = mock.patch.object(bikeep.requests, "post")

        self._credentials_patcher.start()
        self.mock_get = self._mock_get_patcher.start()
        self.mock_post = self._mock_post_patcher.start()

        self.mock_get.side_effect = self._mocked_get
        self.mock_post.side_effect = self._mocked_post

    def tearDown(self):
        self._mock_get_patcher.stop()
        self._mock_post_patcher.stop()
        self._credentials_patcher.stop()
        bikeep._TOKEN_CACHE["access_token"] = None
        bikeep._TOKEN_CACHE["expires_at"] = 0

    def _mocked_get(self, url, headers=None, timeout=10):
        if url.endswith("/location/v1/locations"):
            return _MockResponse(
                {
                    "data": [
                        {
                            "station_id": "station-1",
                            "name": "Mock Station 1",
                            "latitude": 37.7749,
                            "longitude": -122.4194,
                            "devices": {"total": 4},
                        },
                        {
                            "station_id": "station-2",
                            "name": "Mock Station 2",
                            "latitude": 37.7900,
                            "longitude": -122.4100,
                            "devices": {"total": 3},
                        },
                    ]
                }
            )
        if "/device/v1/devices/" in url:
            device_id = url.rsplit("/", 1)[-1]
            if device_id == "unknown-id":
                return _MockResponse({}, status_code=404)
            return _MockResponse(
                {
                    "id": device_id,
                    "name": "Mock Device",
                    "location": {
                        "uri": f"/location/v1/locations/test-station-1"
                    },
                }
            )
        if "/location/v1/locations/" in url:
            location_id = url.rsplit("/", 1)[-1]
            if location_id == "unknown-id":
                return _MockResponse({}, status_code=404)
            return _MockResponse({
                "id": location_id,
                "name": "Mock Station",
                "latitude": 37.7749,
                "longitude": -122.4194,
            })
        return _MockResponse({}, status_code=404)

    def _mocked_post(self, url, headers=None, json=None, data=None, timeout=10):
        if url == bikeep.BIKEEP_AUTH_URL:
            return _MockResponse({"access_token": "mock-token", "expires_in": 3600})

        if "/device/v1/devices/" in url and url.endswith("/commands"):
            command = (json or {}).get("command", "unknown")
            return _MockResponse({"status": "ok", "command": command})

        return _MockResponse({}, status_code=404)

    # ------------------------------------------------------------------
    # List locations (locations)
    # ------------------------------------------------------------------
    def test_list_locations_returns_list(self):
        """get_locations() returns a non-empty list of station dicts."""
        locations = bikeep.get_locations()

        self.assertIsInstance(locations, list)
        self.assertGreater(len(locations), 0, "Expected at least one station from the API")
        logger.info(f"Bikeep returned {len(locations)} location(s)")

    def test_list_locations_count(self):
        """Log and assert the station count is a positive integer."""
        locations = bikeep.get_locations()
        count = len(locations)

        logger.info(f"Location count: {count}")
        self.assertIsInstance(count, int)
        self.assertGreater(count, 0)

    # ------------------------------------------------------------------
    # List devices (docks across all locations)
    # ------------------------------------------------------------------
    def test_list_devices_count(self):
        """Log the total device count and verify it is a positive integer."""
        locations = bikeep.get_locations()
        devices = [s.get("devices", []) for s in locations]
        device_count = sum(d.get("total", 0) for d in devices)

        logger.info(f"Device count: {device_count}")
        self.assertIsInstance(device_count, int)
        self.assertGreater(device_count, 0)

    # ------------------------------------------------------------------
    # Take photo test device
    # ------------------------------------------------------------------
    def test_take_photo_device(self):
        """take_photo() succeeds with mocked Bikeep API."""
        result = bikeep.take_photo(TEST_CAMERA_DEVICE_ID)

        logger.info(f"Take photo result: {result}")
        self.assertIsInstance(result, dict)

    # ------------------------------------------------------------------
    # Lock then unlock (round-trip)
    # ------------------------------------------------------------------
    def test_lock_then_unlock_device(self):
        """Lock followed immediately by unlock against mocked Bikeep API."""
        lock_result = bikeep.lock_dock(TEST_DEVICE_ID)
        logger.info(f"Lock result: {lock_result}")
        self.assertIsInstance(lock_result, dict)

        unlock_result = bikeep.unlock_dock(TEST_DEVICE_ID)
        logger.info(f"Unlock result: {unlock_result}")
        self.assertIsInstance(unlock_result, dict)

    # ------------------------------------------------------------------
    # get_location()
    # ------------------------------------------------------------------

    def test_get_location_returns_lat_lng(self):
        """get_location() returns a location dict for a device ID."""
        result = bikeep.get_location("test-device-1")

        self.assertIn("latitude", result)
        self.assertIn("longitude", result)
        self.assertIsInstance(result["latitude"], float)
        self.assertIsInstance(result["longitude"], float)

    def test_get_location_returns_id_and_name(self):
        """get_location() returns location id and name for a device ID."""
        result = bikeep.get_location("test-device-1")

        self.assertIn("id", result)
        self.assertIn("name", result)
        self.assertEqual(result["id"], "test-station-1")

    def test_get_location_not_found_raises(self):
        """get_location() raises on 404 for an unknown device ID."""
        with self.assertRaises(Exception):
            bikeep.get_location("unknown-id")

if __name__ == "__main__":
    unittest.main()
