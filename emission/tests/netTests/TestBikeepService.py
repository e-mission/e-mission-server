"""
Integration tests for the Bikeep service.

These tests call the real Bikeep API and are intended to verify that the
service functions correctly against the live system.

Assumed initial state of the test device is UNLOCKED with no booking status.
"""

# Standard imports
from builtins import *
import unittest
import os
import time
import json
import logging
import functools
import requests
import urllib3

# Module under test
import emission.net.ext_service.bikeep.bikeep_service as bikeep

logger = logging.getLogger(__name__)


TEST_DEVICE_ID = os.environ.get("BIKEEP_TEST_DEVICE_ID", None)
TEST_CAMERA_DEVICE_ID = os.environ.get("BIKEEP_TEST_CAMERA_DEVICE_ID", None)

SKIP_IF_NO_TEST_DEVICE = unittest.skipIf(
    TEST_DEVICE_ID is None,
    "TEST_DEVICE_ID is not set — set the environment variable BIKEEP_TEST_DEVICE_ID to a real dock ID to run device tests"
)

SKIP_IF_NO_CAMERA = unittest.skipIf(
    TEST_CAMERA_DEVICE_ID is None,
    "TEST_CAMERA_DEVICE_ID is not set — set the environment variable BIKEEP_TEST_CAMERA_DEVICE_ID to a real camera-capable device ID to run photo tests"
)


class TestBikeepServiceIntegration(unittest.TestCase):
    """
    Integration tests that call the real Bikeep API.

    Tests are skipped automatically when credentials are absent so the suite
    stays green in CI environments that don't have API access.
    """

    def setUp(self):
        # Suppress the InsecureRequestWarning that fires when verify=False.
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Wrap requests.get/post so every call in the service goes out with
        # verify=False — avoids SSL errors against self-signed certs in local
        # dev without modifying production code.
        _real_get = requests.get
        _real_post = requests.post
        requests.get  = functools.partial(_real_get,  verify=False)
        requests.post = functools.partial(_real_post, verify=False)
        self._real_get  = _real_get
        self._real_post = _real_post

        # Clear the token cache before each test so we exercise the full
        # auth flow at least once per run.
        bikeep._TOKEN_CACHE["access_token"] = None
        bikeep._TOKEN_CACHE["expires_at"] = 0

    def tearDown(self):
        # Restore original request functions
        requests.get  = self._real_get
        requests.post = self._real_post
        bikeep._TOKEN_CACHE["access_token"] = None
        bikeep._TOKEN_CACHE["expires_at"] = 0

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
    @SKIP_IF_NO_CAMERA
    def test_take_photo_device(self):
        """take_photo() succeeds against the real API for TEST_CAMERA_DEVICE_ID."""
        time.sleep(1)
        result = bikeep.take_photo(TEST_CAMERA_DEVICE_ID)

        logger.info(f"Take photo result: {result}")
        self.assertIsInstance(result, dict)

    # ------------------------------------------------------------------
    # Lock then unlock (round-trip)
    # ------------------------------------------------------------------
    @SKIP_IF_NO_TEST_DEVICE
    def test_lock_then_unlock_device(self):
        """Lock followed immediately by unlock against the real API."""
        time.sleep(1)
        lock_result = bikeep.lock_dock(TEST_DEVICE_ID)
        logger.info(f"Lock result: {lock_result}")
        self.assertIsInstance(lock_result, dict)

        time.sleep(1)
        unlock_result = bikeep.unlock_dock(TEST_DEVICE_ID)
        logger.info(f"Unlock result: {unlock_result}")
        self.assertIsInstance(unlock_result, dict)

    # ------------------------------------------------------------------
    # Book then cancel booking (round-trip)
    # ------------------------------------------------------------------
    @SKIP_IF_NO_TEST_DEVICE
    def test_book_then_cancel_booking_device(self):
        """Book followed by cancel-booking against the real API."""
        time.sleep(1)
        book_result = bikeep.book_dock(TEST_DEVICE_ID)
        logger.info(f"Book result: {book_result}")
        self.assertIsInstance(book_result, dict)

        time.sleep(1)
        cancel_result = bikeep.cancel_booking_dock(TEST_DEVICE_ID)
        logger.info(f"Cancel booking result: {cancel_result}")
        self.assertIsInstance(cancel_result, dict)


if __name__ == "__main__":
    unittest.main()
