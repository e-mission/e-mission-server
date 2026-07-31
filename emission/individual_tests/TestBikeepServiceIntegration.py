"""
Live integration tests for the Bikeep service.

These tests call the real Bikeep API and are skipped unless
RUN_BIKEEP_LIVE_TESTS is enabled in the environment.
"""

from builtins import *
import functools
import logging
import os
import time
import unittest

import requests
import urllib3

import emission.net.ext_service.bikeep.bikeep_service as bikeep

logger = logging.getLogger(__name__)


def _env_flag_enabled(name):
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


RUN_BIKEEP_LIVE_TESTS = _env_flag_enabled("RUN_BIKEEP_LIVE_TESTS")
TEST_DEVICE_ID = os.environ.get("BIKEEP_TEST_DEVICE_ID")
TEST_CAMERA_DEVICE_ID = os.environ.get("BIKEEP_TEST_CAMERA_DEVICE_ID")

SKIP_IF_NO_TEST_DEVICE = unittest.skipIf(
    TEST_DEVICE_ID is None,
    "BIKEEP_TEST_DEVICE_ID is not set; set it to a real dock ID to run live device tests",
)

SKIP_IF_NO_CAMERA = unittest.skipIf(
    TEST_CAMERA_DEVICE_ID is None,
    "BIKEEP_TEST_CAMERA_DEVICE_ID is not set; set it to a real camera device ID to run live photo tests",
)


@unittest.skipUnless(
    RUN_BIKEEP_LIVE_TESTS,
    "Set RUN_BIKEEP_LIVE_TESTS=1 to run live Bikeep integration tests",
)
class TestBikeepServiceIntegration(unittest.TestCase):
    """Bikeep integration tests that exercise the live external API."""

    def setUp(self):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        real_get = requests.get
        real_post = requests.post
        requests.get = functools.partial(real_get, verify=False)
        requests.post = functools.partial(real_post, verify=False)
        self._real_get = real_get
        self._real_post = real_post

        bikeep._TOKEN_CACHE["access_token"] = None
        bikeep._TOKEN_CACHE["expires_at"] = 0

    def tearDown(self):
        requests.get = self._real_get
        requests.post = self._real_post
        bikeep._TOKEN_CACHE["access_token"] = None
        bikeep._TOKEN_CACHE["expires_at"] = 0

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

    def test_list_devices_count(self):
        """Log the total device count and verify it is a positive integer."""
        locations = bikeep.get_locations()
        devices = [station.get("devices", []) for station in locations]
        device_count = sum(device.get("total", 0) for device in devices)

        logger.info(f"Device count: {device_count}")
        self.assertIsInstance(device_count, int)
        self.assertGreater(device_count, 0)

    @SKIP_IF_NO_CAMERA
    def test_take_photo_device(self):
        """take_photo() succeeds against the real API for TEST_CAMERA_DEVICE_ID."""
        time.sleep(1)
        result = bikeep.take_photo(TEST_CAMERA_DEVICE_ID)

        logger.info(f"Take photo result: {result}")
        self.assertIsInstance(result, dict)

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