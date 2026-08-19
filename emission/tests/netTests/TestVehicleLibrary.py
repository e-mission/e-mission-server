# Standard imports
from builtins import *
import unittest
import uuid
import time
import logging
from unittest.mock import patch

# Our imports
import emission.core.get_database as edb
import emission.core.wrapper.rental as ecwr
import emission.net.api.vehicle_library as vl
import emission.storage.timeseries.abstract_timeseries as esta

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
        self.state_db = edb.get_state_db()
        self.timeseries_db = edb.get_timeseries_db()

        # Clean up any previous test data

        self.mock_db.delete_many({'vehicle_id': VEHICLE_ID})
        self.mock_db.delete_many({'location': str(self.test_uuid)})
        self.profile_db.delete_many({'user_id': self.test_uuid})
        self.state_db.delete_many({'user_id': self.test_uuid})
        self.timeseries_db.delete_many({'user_id': self.test_uuid, 'metadata.key': vl.VEHICLE_RENTAL_KEY})

    def tearDown(self):
        """Clean up test data from MongoDB."""
        self.mock_db.delete_many({'vehicle_id': VEHICLE_ID})
        self.mock_db.delete_many({'location': str(self.test_uuid)})
        self.profile_db.delete_many({'user_id': self.test_uuid})
        self.state_db.delete_many({'user_id': self.test_uuid})
        self.timeseries_db.delete_many({'user_id': self.test_uuid, 'metadata.key': vl.VEHICLE_RENTAL_KEY})

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _insert_vehicle(self, location=DOCK_ID):
        """Insert a test vehicle into the real MongoDB."""
        now = _now()
        doc = {
            'vehicle_id': VEHICLE_ID,
            'location': location,
            'created_at': now,
            'updated_at': now,
        }
        self.mock_db.insert_one(doc)
        return doc

    def _insert_active_rental(self, payment_hold_info=None, rental_start_ts=None):
        """Insert an active rental entry into the user's timeseries."""
        start_ts = rental_start_ts if rental_start_ts is not None else _now()
        if payment_hold_info is None:
            payment_hold_info = {'id': 'pi_hold_123'}
        rental_state = ecwr.Rental({
            'vehicle_id': VEHICLE_ID,
            'vehicle_name': 'test vehicle',
            'payment_hold_info': payment_hold_info,
            'start_ts': start_ts,
            'end_ts': None,
            'rental_status': 'active',
        })
        esta.TimeSeries.get_time_series(self.test_uuid).insert_data(
            self.test_uuid,
            vl.VEHICLE_RENTAL_KEY,
            rental_state,
        )
        return rental_state

    def _get_latest_rental_entry(self):
        entries = esta.TimeSeries.get_time_series(self.test_uuid).find_entries([vl.VEHICLE_RENTAL_KEY])
        if len(entries) == 0:
            return None
        return entries[-1]

    def _checkout_vehicle(self, hold_amount_cents=vl.DEFAULT_HOLD_AMOUNT_CENTS):
        """Call checkout_vehicle with the current explicit signature."""
        return vl.checkout_vehicle(self.test_uuid, VEHICLE_ID, hold_amount_cents)

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
        """checkout_vehicle() clears location from dock once bike is checked out."""
        self._insert_vehicle()

        with patch.object(vl.ss, 'create_hold_payment_intent', return_value={'id': 'pi_hold_123'}), \
             patch.object(vl.bikeep_service, 'unlock_dock', return_value={}):
            result = self._checkout_vehicle()

        self.assertEqual(result['result'], 'checked_out')

        # Verify vehicle location is now unavailable (not docked)
        vehicle = self.mock_db.find_one({'vehicle_id': VEHICLE_ID})
        self.assertIsNone(vehicle['location'])

    def test_checkout_vehicle_records_checkout_timestamp(self):
        """checkout_vehicle() sets checkout_ts to current time."""
        self._insert_vehicle()
        before = _now()

        with patch.object(vl.ss, 'create_hold_payment_intent', return_value={'id': 'pi_hold_123'}), \
             patch.object(vl.bikeep_service, 'unlock_dock', return_value={}):
            self._checkout_vehicle()

        after = _now()
        vehicle = self.mock_db.find_one({'vehicle_id': VEHICLE_ID})
        checkout_ts = vehicle['checkout_ts']
        self.assertIsNotNone(checkout_ts)
        self.assertGreaterEqual(checkout_ts, before)
        self.assertLessEqual(checkout_ts, after)

    def test_checkout_vehicle_calls_bikeep_unlock(self):
        """checkout_vehicle() unlocks the dock via bikeep."""
        self._insert_vehicle()

        with patch.object(vl.ss, 'create_hold_payment_intent', return_value={'id': 'pi_hold_123'}), \
             patch.object(vl.bikeep_service, 'unlock_dock', return_value={}) as mock_unlock:
            self._checkout_vehicle()

        mock_unlock.assert_called_once_with(DOCK_ID)

    def test_checkout_vehicle_persists_active_rental_entry(self):
        """checkout_vehicle() stores the active vehicle-user mapping in manual/vehicle_rental."""
        self._insert_vehicle()

        with patch.object(vl.ss, 'create_hold_payment_intent', return_value={'id': 'pi_hold_123'}), \
             patch.object(vl.bikeep_service, 'unlock_dock', return_value={}):
            self._checkout_vehicle()

        rental_entry = self._get_latest_rental_entry()
        self.assertIsNotNone(rental_entry)
        rental_state = rental_entry['data']
        self.assertEqual(rental_state['vehicle_id'], VEHICLE_ID)
        self.assertEqual(rental_state['payment_hold_info']['id'], 'pi_hold_123')
        self.assertEqual(rental_state['rental_status'], 'active')
        self.assertIsNotNone(rental_state['start_ts'])
        self.assertIsNone(rental_state.get('end_ts'))

    # ------------------------------------------------------------------
    # check_in_vehicle()
    # ------------------------------------------------------------------

    def test_checkin_vehicle_resets_location_to_dock(self):
        """check_in_vehicle() sets location back to dock_id (vehicle returned)."""
        # Vehicle is checked out (location = user UUID)
        self._insert_vehicle(
            location=str(self.test_uuid),
        )
        self._insert_active_rental(rental_start_ts=_now())

        with patch.object(vl.bikeep_service, 'lock_dock', return_value={}), \
               patch.object(vl.ss, 'capture_hold_payment_intent', return_value={'id': 'pi_hold_123', 'status': 'succeeded'}):
            result = vl.check_in_vehicle(self.test_uuid, ALT_DOCK_ID)

        self.assertEqual(result['result'], 'checked_in')

        # Verify vehicle is back at dock
        vehicle = self.mock_db.find_one({'vehicle_id': VEHICLE_ID})
        self.assertEqual(vehicle['location'], ALT_DOCK_ID)

    def test_checkin_vehicle_marks_rental_completed_with_timestamps(self):
        """check_in_vehicle() marks rental completed and sets end timestamp while preserving start timestamp."""
        self._insert_vehicle(
            location=str(self.test_uuid),
        )
        rental_start_ts = _now()
        self._insert_active_rental(payment_hold_info={'id': 'pi_hold_123'}, rental_start_ts=rental_start_ts)

        with patch.object(vl.bikeep_service, 'lock_dock', return_value={}), \
               patch.object(vl.ss, 'capture_hold_payment_intent', return_value={'id': 'pi_hold_123', 'status': 'succeeded'}):
            vl.check_in_vehicle(self.test_uuid, ALT_DOCK_ID)

        rental_entry = self._get_latest_rental_entry()
        rental_state = rental_entry['data']
        self.assertEqual(rental_state['vehicle_id'], VEHICLE_ID)
        self.assertEqual(rental_state['payment_hold_info']['id'], 'pi_hold_123')
        self.assertEqual(rental_state['rental_status'], 'completed')
        self.assertEqual(rental_state['start_ts'], rental_start_ts)
        self.assertIsNotNone(rental_state['end_ts'])
        self.assertGreaterEqual(rental_state['end_ts'], rental_start_ts)

    def test_checkin_vehicle_calls_bikeep_lock(self):
        """check_in_vehicle() locks the dock via bikeep."""
        self._insert_vehicle(
            location=str(self.test_uuid),
        )
        self._insert_active_rental(rental_start_ts=_now())

        with patch.object(vl.bikeep_service, 'lock_dock', return_value={}) as mock_lock, \
             patch.object(vl.ss, 'capture_hold_payment_intent', return_value={'id': 'pi_hold_123', 'status': 'succeeded'}):
            vl.check_in_vehicle(self.test_uuid, ALT_DOCK_ID)

        mock_lock.assert_called_once_with(ALT_DOCK_ID)

    def test_checkin_vehicle_captures_payment_hold_with_computed_amount(self):
        """check_in_vehicle() captures the active Stripe hold after locking using computed fee tiers."""
        self._insert_vehicle(
            location=str(self.test_uuid),
        )
        fixed_now = 2000000
        # 6 hours should map to $35 -> 3500 cents
        self._insert_active_rental(payment_hold_info={'id': 'pi_hold_456'}, rental_start_ts=fixed_now - (6 * 60 * 60))

        with patch.object(vl, 'time') as mock_time, \
             patch.object(vl.bikeep_service, 'lock_dock', return_value={}), \
             patch.object(vl.ss, 'capture_hold_payment_intent', return_value={'id': 'pi_hold_456', 'status': 'succeeded'}) as mock_capture:
            mock_time.time.return_value = fixed_now
            vl.check_in_vehicle(self.test_uuid, ALT_DOCK_ID)

        mock_capture.assert_called_once_with('pi_hold_456', 3500)

    def test_checkin_vehicle_nothing_checked_out_returns_403(self):
        """check_in_vehicle() rejects if user has no vehicle checked out."""
        # Vehicle is at a dock, not checked out by this user
        self._insert_vehicle(location=DOCK_ID)

        with self.assertRaises(ValueError) as ctx:
            vl.check_in_vehicle(self.test_uuid, ALT_DOCK_ID)
        self.assertEqual(ctx.exception.args[0], 403)

    def test_checkin_vehicle_bikeep_failure_does_not_update_db(self):
        """check_in_vehicle() does not update DB if bikeep.lock_dock fails."""
        self._insert_vehicle(
            location=str(self.test_uuid),
        )
        self._insert_active_rental(rental_start_ts=_now())

        with patch.object(vl.bikeep_service, 'lock_dock', side_effect=RuntimeError("lock failed")):
            with self.assertRaises(RuntimeError):
                vl.check_in_vehicle(self.test_uuid, ALT_DOCK_ID)

        # Vehicle should remain unchanged in DB
        vehicle = self.mock_db.find_one({'vehicle_id': VEHICLE_ID})
        self.assertEqual(vehicle['location'], str(self.test_uuid))

        rental_entry = self._get_latest_rental_entry()
        self.assertEqual(rental_entry['data']['rental_status'], 'active')

    # ------------------------------------------------------------------
    # Integration: full workflow
    # ------------------------------------------------------------------

    def test_full_workflow_reserve_checkout_checkin(self):
        """Full workflow: vehicle at dock → checked out → checked in."""
        # Start: vehicle at dock
        self._insert_vehicle(location=DOCK_ID)

        # Checkout
        with patch.object(vl.ss, 'create_hold_payment_intent', return_value={'id': 'pi_hold_123'}), \
             patch.object(vl.bikeep_service, 'unlock_dock', return_value={}):
            self._checkout_vehicle()

        # After checkout: vehicle no longer mapped to a dock location
        vehicle = self.mock_db.find_one({'vehicle_id': VEHICLE_ID})
        self.assertIsNone(vehicle['location'])

        # Check-in
        with patch.object(vl.bikeep_service, 'lock_dock', return_value={}), \
               patch.object(vl.ss, 'capture_hold_payment_intent', return_value={'id': 'pi_hold_123', 'status': 'succeeded'}):
            vl.check_in_vehicle(self.test_uuid, ALT_DOCK_ID)

        # After check-in: vehicle back at dock
        vehicle = self.mock_db.find_one({'vehicle_id': VEHICLE_ID})
        self.assertEqual(vehicle['location'], ALT_DOCK_ID)
        self.assertFalse('reservation' in vehicle)


if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
