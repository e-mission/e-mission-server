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
                            "devices": {"total": 4},
                        },
                        {
                            "station_id": "station-2",
                            "name": "Mock Station 2",
                            "devices": {"total": 3},
                        },
                    ]
                }
            )
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
    # Book then cancel booking (round-trip)
    # ------------------------------------------------------------------
    def test_book_then_cancel_booking_device(self):
        """Book followed by cancel-booking against mocked Bikeep API."""
        book_result = bikeep.book_dock(TEST_DEVICE_ID)
        logger.info(f"Book result: {book_result}")
        self.assertIsInstance(book_result, dict)

        cancel_result = bikeep.cancel_booking_dock(TEST_DEVICE_ID)
        logger.info(f"Cancel booking result: {cancel_result}")
        self.assertIsInstance(cancel_result, dict)


if __name__ == "__main__":
    unittest.main()
