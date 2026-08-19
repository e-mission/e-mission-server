# Standard imports
from builtins import *
import unittest
import uuid
import time
import logging
from unittest.mock import MagicMock, patch

# Our imports
import emission.core.get_database as edb
import emission.net.api.vehicle_library as vl

logger = logging.getLogger(__name__)

VEHICLE_ID = "test-bike-001"
DOCK_ID = "test-dock-1"
ALT_DOCK_ID = "test-dock-2"


def _now():
    return time.time()


class TestVehicleLibrary(unittest.TestCase):
    """
    Tests for vehicle_library functions.
    Uses real MongoDB but mocks the bikeep API.
    Focus: verify correct vehicle state transitions and persistence.
    """

    def setUp(self):
        """Set up test vehicles in MongoDB."""
        self.test_uuid = uuid.uuid4()
        self.mock_db = edb.get_vehicle_db()
        self.profile_db = edb.get_profile_db()

        # Clean up any previous test data

        self.mock_db.delete_many({'vehicle_id': VEHICLE_ID})
        self.mock_db.delete_many({'location': str(self.test_uuid)})
        self.profile_db.delete_many({'user_id': self.test_uuid})

    def tearDown(self):
        """Clean up test data from MongoDB."""
        self.mock_db.delete_many({'vehicle_id': VEHICLE_ID})
        self.mock_db.delete_many({'location': str(self.test_uuid)})
        self.profile_db.delete_many({'user_id': self.test_uuid})

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _insert_vehicle(self, location=DOCK_ID, reservation=None):
        """Insert a test vehicle into the real MongoDB."""
        now = _now()
        doc = {
            'vehicle_id': VEHICLE_ID,
            'location': location,
            'reservation': reservation,
            'created_at': now,
            'updated_at': now,
        }
        self.mock_db.insert_one(doc)
        return doc

    def _active_reservation(self, user_uuid=None, checkout_ts=None):
        """Return a reservation dict that has not yet expired."""
        return {
            'user_uuid': str(user_uuid or self.test_uuid),
            'expires_ts': _now() + 3600,
            'checkout_ts': checkout_ts,
            'original_dock_id': DOCK_ID,
            'charge_id': None,
        }

    def _expired_reservation(self):
        """Return a reservation dict that is already past its expiry."""
        return {
            'user_uuid': str(self.test_uuid),
            'expires_ts': _now() - 1,
            'checkout_ts': None,
            'original_dock_id': DOCK_ID,
            'charge_id': None,
        }

    def _make_request_mock(self, body):
        """Mock bottle.request with the given JSON body."""
        req = MagicMock()
        req.json = body
        return req

    # ------------------------------------------------------------------
    # stations()
    # ------------------------------------------------------------------

    def test_stations_returns_bikeep_locations(self):
        """stations() delegates to bikeep_service.get_locations() and returns result."""
        expected = [{'station_id': '1', 'name': 'Main St', 'docks': []}]
        with patch.object(vl.bikeep_service, 'get_locations', return_value=expected):
            result = vl.stations()
        self.assertEqual(result, expected)

    def test_stations_propagates_bikeep_exception(self):
        """stations() propagates exceptions from bikeep_service."""
        with patch.object(vl.bikeep_service, 'get_locations', side_effect=RuntimeError("API down")):
            with self.assertRaises(RuntimeError):
                vl.stations()

    # ------------------------------------------------------------------
    # checkout_vehicle()
    # ------------------------------------------------------------------

    def test_checkout_vehicle_moves_location_to_user_uuid(self):
        """checkout_vehicle() sets location to user UUID (vehicle checked out)."""
        self._insert_vehicle(reservation=self._active_reservation())
        req_mock = self._make_request_mock({'vehicle_id': VEHICLE_ID})

        with patch.object(vl, 'request', req_mock), \
             patch.object(vl.bikeep_service, 'unlock_dock', return_value={}):
            result = vl.checkout_vehicle(self.test_uuid)

        self.assertEqual(result['result'], 'checked_out')

        # Verify vehicle location is now user UUID
        vehicle = self.mock_db.find_one({'vehicle_id': VEHICLE_ID})
        self.assertEqual(vehicle['location'], str(self.test_uuid))

    def test_checkout_vehicle_records_checkout_timestamp(self):
        """checkout_vehicle() sets reservation.checkout_ts to current time."""
        self._insert_vehicle(reservation=self._active_reservation())
        req_mock = self._make_request_mock({'vehicle_id': VEHICLE_ID})
        before = _now()

        with patch.object(vl, 'request', req_mock), \
             patch.object(vl.bikeep_service, 'unlock_dock', return_value={}):
            vl.checkout_vehicle(self.test_uuid)

        after = _now()
        vehicle = self.mock_db.find_one({'vehicle_id': VEHICLE_ID})
        checkout_ts = vehicle['checkout_ts']
        self.assertIsNotNone(checkout_ts)
        self.assertGreaterEqual(checkout_ts, before)
        self.assertLessEqual(checkout_ts, after)

    def test_checkout_vehicle_calls_bikeep_unlock(self):
        """checkout_vehicle() unlocks the dock via bikeep."""
        self._insert_vehicle(reservation=self._active_reservation())
        req_mock = self._make_request_mock({'vehicle_id': VEHICLE_ID})

        with patch.object(vl, 'request', req_mock), \
             patch.object(vl.bikeep_service, 'unlock_dock', return_value={}) as mock_unlock:
            vl.checkout_vehicle(self.test_uuid)

        mock_unlock.assert_called_once_with(DOCK_ID)

    # ------------------------------------------------------------------
    # check_in_vehicle()
    # ------------------------------------------------------------------

    def test_checkin_vehicle_resets_location_to_dock(self):
        """check_in_vehicle() sets location back to dock_id (vehicle returned)."""
        # Vehicle is checked out (location = user UUID)
        self._insert_vehicle(
            location=str(self.test_uuid),
            reservation=self._active_reservation(checkout_ts=_now()),
        )
        req_mock = self._make_request_mock({'dock_id': ALT_DOCK_ID})

        with patch.object(vl, 'request', req_mock), \
             patch.object(vl.bikeep_service, 'lock_dock', return_value={}):
            result = vl.check_in_vehicle(self.test_uuid)

        self.assertEqual(result['result'], 'checked_in')

        # Verify vehicle is back at dock
        vehicle = self.mock_db.find_one({'vehicle_id': VEHICLE_ID})
        self.assertEqual(vehicle['location'], ALT_DOCK_ID)

    def test_checkin_vehicle_clears_reservation(self):
        """check_in_vehicle() clears (sets to None) the reservation."""
        self._insert_vehicle(
            location=str(self.test_uuid),
            reservation=self._active_reservation(checkout_ts=_now()),
        )
        req_mock = self._make_request_mock({'dock_id': ALT_DOCK_ID})

        with patch.object(vl, 'request', req_mock), \
             patch.object(vl.bikeep_service, 'lock_dock', return_value={}):
            vl.check_in_vehicle(self.test_uuid)

        vehicle = self.mock_db.find_one({'vehicle_id': VEHICLE_ID})
        self.assertIsNone(vehicle['reservation'])

    def test_checkin_vehicle_calls_bikeep_lock(self):
        """check_in_vehicle() locks the dock via bikeep."""
        self._insert_vehicle(
            location=str(self.test_uuid),
            reservation=self._active_reservation(checkout_ts=_now()),
        )
        req_mock = self._make_request_mock({'dock_id': ALT_DOCK_ID})

        with patch.object(vl, 'request', req_mock), \
             patch.object(vl.bikeep_service, 'lock_dock', return_value={}) as mock_lock:
            vl.check_in_vehicle(self.test_uuid)

        mock_lock.assert_called_once_with(ALT_DOCK_ID)

    def test_checkin_vehicle_nothing_checked_out_returns_403(self):
        """check_in_vehicle() rejects if user has no vehicle checked out."""
        from emission.net.api.bottle import HTTPError
        # Vehicle is at a dock, not checked out by this user
        self._insert_vehicle(location=DOCK_ID)
        req_mock = self._make_request_mock({'dock_id': ALT_DOCK_ID})

        with patch.object(vl, 'request', req_mock):
            with self.assertRaises(HTTPError) as ctx:
                vl.check_in_vehicle(self.test_uuid)
        self.assertEqual(ctx.exception.status_code, 403)

    def test_checkin_vehicle_missing_dock_id_returns_400(self):
        """check_in_vehicle() rejects missing dock_id."""
        from emission.net.api.bottle import HTTPError
        req_mock = self._make_request_mock({})

        with patch.object(vl, 'request', req_mock):
            with self.assertRaises(HTTPError) as ctx:
                vl.check_in_vehicle(self.test_uuid)
        self.assertEqual(ctx.exception.status_code, 400)

    def test_checkin_vehicle_bikeep_failure_does_not_update_db(self):
        """check_in_vehicle() does not update DB if bikeep.lock_dock fails."""
        self._insert_vehicle(
            location=str(self.test_uuid),
            reservation=self._active_reservation(checkout_ts=_now()),
        )
        req_mock = self._make_request_mock({'dock_id': ALT_DOCK_ID})

        with patch.object(vl, 'request', req_mock), \
             patch.object(vl.bikeep_service, 'lock_dock', side_effect=RuntimeError("lock failed")):
            with self.assertRaises(RuntimeError):
                vl.check_in_vehicle(self.test_uuid)

        # Vehicle should remain unchanged in DB
        vehicle = self.mock_db.find_one({'vehicle_id': VEHICLE_ID})
        self.assertEqual(vehicle['location'], str(self.test_uuid))
        self.assertIsNotNone(vehicle['reservation'])

    # ------------------------------------------------------------------
    # Integration: full workflow
    # ------------------------------------------------------------------

    def test_full_workflow_reserve_checkout_checkin(self):
        """Full workflow: vehicle at dock → checked out → checked in."""
        # Start: vehicle at dock
        self._insert_vehicle(location=DOCK_ID)

        # Checkout
        req_checkout = self._make_request_mock({'vehicle_id': VEHICLE_ID})
        with patch.object(vl, 'request', req_checkout), \
             patch.object(vl.bikeep_service, 'unlock_dock', return_value={}):
            vl.checkout_vehicle(self.test_uuid)

        # After checkout: vehicle location is user UUID
        vehicle = self.mock_db.find_one({'vehicle_id': VEHICLE_ID})
        self.assertEqual(vehicle['location'], str(self.test_uuid))

        # Check-in
        req_checkin = self._make_request_mock({'dock_id': ALT_DOCK_ID})
        with patch.object(vl, 'request', req_checkin), \
             patch.object(vl.bikeep_service, 'lock_dock', return_value={}):
            vl.check_in_vehicle(self.test_uuid)

        # After check-in: vehicle back at dock, reservation cleared
        vehicle = self.mock_db.find_one({'vehicle_id': VEHICLE_ID})
        self.assertEqual(vehicle['location'], ALT_DOCK_ID)
        self.assertIsNone(vehicle['reservation'])


if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
