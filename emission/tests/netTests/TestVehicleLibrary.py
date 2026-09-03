# Standard imports
from builtins import *
import unittest
import uuid
import time
import logging
import os
from unittest.mock import patch

import arrow

# Our imports
import emission.core.get_database as edb
import emission.core.wrapper.rental as ecwr
import emission.core.wrapper.localdate as ecwld

os.environ.setdefault('STRIPE_SECRET_KEY', 'sk_test_dummy')

import emission.net.api.vehicle_library as vl
import emission.storage.timeseries.abstract_timeseries as esta

logger = logging.getLogger(__name__)

VEHICLE_ID = "test-bike-001"
DOCK_ID = "test-dock-1"
ALT_DOCK_ID = "test-dock-2"

_MOCK_DOCK_LOC = {'id': DOCK_ID, 'name': 'Test Dock', 'latitude': 37.7749, 'longitude': -122.4194}
_MOCK_ALT_DOCK_LOC = {'id': ALT_DOCK_ID, 'name': 'Alt Test Dock', 'latitude': 37.7900, 'longitude': -122.4100}


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
        self._default_fee_config = {
            'vehicle_library': {
                'fee_expression': "(5 if duration <= 5 else 35 if duration <= 24 else 100 if duration <= 72 else 200 if duration <= 144 else 380) * (0.5 if subgroup == 'discount' else 1) * (1.5 if baseMode == 'E_BIKE' else 1)",
            }
        }
        self._fee_config_patcher = patch.object(vl.edc, 'get_deployment_config', return_value=self._default_fee_config)
        self._fee_config_patcher.start()
        self._dock_code_patcher = patch.object(vl.bikeep_service, 'get_device_id_for_code', side_effect=lambda dock_code: dock_code)
        self._dock_code_patcher.start()
        self._sandbox_patcher = patch.object(vl.ss, 'STRIPE_IS_SANDBOX', True)
        self._sandbox_patcher.start()

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
        self._dock_code_patcher.stop()
        self._fee_config_patcher.stop()
        self._sandbox_patcher.stop()

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
        timezone = "America/Los_Angeles"
        if payment_hold_info is None:
            payment_hold_info = {'id': 'pi_hold_123'}
        rental_state = ecwr.Rental({
            'vehicle_id': VEHICLE_ID,
            'vehicle_name': 'test vehicle',
            'payment_hold_info': payment_hold_info,
            'start_ts': start_ts,
            'start_local_dt': ecwld.LocalDate.get_local_date(start_ts, timezone),
            'start_fmt_time': arrow.get(start_ts).to(timezone).isoformat(),
            'start_dock_id': DOCK_ID,
            'end_ts': None,
            'end_local_dt': None,
            'end_fmt_time': None,
            'end_dock_id': None,
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
        """stations() returns Bikeep's locations (wrapped), annotated with rentable_vehicles."""
        expected = [{'id': 'loc-1', 'name': 'Main St', 'devices': {'total': 1, 'available': 0}}]
        with patch.object(vl.bikeep_service, 'get_locations_and_all_devices', return_value=(expected, [])):
            result = vl.stations()
        self.assertEqual(len(result['stations']), 1)
        self.assertEqual(result['stations'][0]['devices']['rentable_vehicles'], 0)

    def test_stations_counts_only_locked_docks_with_our_fleet_vehicles(self):
        """A location's rentable_vehicles only counts LOCKED docks holding one of our
        own vehicles - not empty docks, and not docks holding the public's own bikes
        (no matching fleet vehicle in our DB). Fleet vehicles are matched by dock
        code (Vehicle.location), not the real Bikeep device id."""
        self._insert_vehicle(location='code-ours')

        locations = [{'id': 'loc-1', 'name': 'Main St'}]
        devices = [
            {'id': 'real-id-ours', 'code': 'code-ours', 'location': {'id': 'loc-1'}, 'state': {'value': 'LOCKED'}},  # our bike
            {'id': 'real-id-public', 'code': 'code-public', 'location': {'id': 'loc-1'}, 'state': {'value': 'LOCKED'}},  # public's own bike
            {'id': 'real-id-empty', 'code': 'code-empty', 'location': {'id': 'loc-1'}, 'state': {'value': 'UNLOCKED'}},  # empty dock
        ]
        with patch.object(vl.bikeep_service, 'get_locations_and_all_devices', return_value=(locations, devices)):
            result = vl.stations()

        self.assertEqual(result['stations'][0]['devices']['rentable_vehicles'], 1)

    def test_stations_excludes_checked_out_vehicles(self):
        """A vehicle with location=None (checked out) doesn't count toward any dock."""
        self._insert_vehicle(location=None)

        locations = [{'id': 'loc-1', 'name': 'Main St'}]
        devices = [{'id': 'real-id-ours', 'code': 'code-ours', 'location': {'id': 'loc-1'}, 'state': {'value': 'LOCKED'}}]
        with patch.object(vl.bikeep_service, 'get_locations_and_all_devices', return_value=(locations, devices)):
            result = vl.stations()

        self.assertEqual(result['stations'][0]['devices']['rentable_vehicles'], 0)

    def test_stations_reports_zero_rentable_vehicles_for_offline_locations(self):
        """An offline location can't reliably report device state, so it should
        never be reported as having rentable vehicles, even if our fleet has a
        vehicle recorded there."""
        self._insert_vehicle(location='code-ours')

        locations = [{'id': 'loc-1', 'name': 'Main St', 'connection': 'offline'}]
        devices = [{'id': 'real-id-ours', 'code': 'code-ours', 'location': {'id': 'loc-1'}, 'state': {'value': 'LOCKED'}}]
        with patch.object(vl.bikeep_service, 'get_locations_and_all_devices', return_value=(locations, devices)):
            result = vl.stations()

        self.assertEqual(result['stations'][0]['devices']['rentable_vehicles'], 0)

    def test_stations_propagates_bikeep_exception(self):
        """stations() propagates exceptions from bikeep_service."""
        with patch.object(vl.bikeep_service, 'get_locations_and_all_devices', side_effect=RuntimeError("API down")):
            with self.assertRaises(RuntimeError):
                vl.stations()

    def test_blank_state_check_pending_setup_and_stations_do_not_raise(self):
        """Blank payment state and blank vehicle DB should not cause exceptions when querying setup status or stations."""
        blank_user_uuid = uuid.uuid4()
        existing_vehicles = [dict(vehicle) for vehicle in self.mock_db.find()]

        try:
            self.mock_db.delete_many({})
            self.state_db.delete_many({'user_id': blank_user_uuid})
            self.profile_db.delete_many({'user_id': blank_user_uuid})
            self.timeseries_db.delete_many({'user_id': blank_user_uuid, 'metadata.key': vl.VEHICLE_RENTAL_KEY})

            with patch.object(vl.bikeep_service, 'get_locations_and_all_devices', return_value=([], [])):
                setup_status = vl.check_and_get_pending_setup_status(blank_user_uuid)
                stations_result = vl.stations()

            self.assertIsInstance(setup_status, dict)
            self.assertIn('payment_setup_status', setup_status)
            self.assertIn('is_sandbox', setup_status)
            self.assertEqual(stations_result, {'stations': []})
        finally:
            self.mock_db.delete_many({})
            if existing_vehicles:
                self.mock_db.insert_many(existing_vehicles)

    def test_blank_vehicle_db_with_bikeep_stations_returns_zero_rentable_vehicles(self):
        """If Bikeep reports stations but our vehicle DB is blank, stations() should still return them with zero rentable vehicles."""
        existing_vehicles = [dict(vehicle) for vehicle in self.mock_db.find()]
        bikeep_locations = [{'id': 'loc-1', 'name': 'Main St'}]
        bikeep_devices = [
            {'id': 'real-id-ours', 'code': 'code-ours', 'location': {'id': 'loc-1'}, 'state': {'value': 'LOCKED'}},
            {'id': 'real-id-empty', 'code': 'code-empty', 'location': {'id': 'loc-1'}, 'state': {'value': 'UNLOCKED'}},
        ]

        try:
            self.mock_db.delete_many({})

            with patch.object(vl.bikeep_service, 'get_locations_and_all_devices', return_value=(bikeep_locations, bikeep_devices)):
                result = vl.stations()

            self.assertEqual(len(result['stations']), 1)
            self.assertEqual(result['stations'][0]['id'], 'loc-1')
            self.assertEqual(result['stations'][0]['devices']['rentable_vehicles'], 0)
        finally:
            self.mock_db.delete_many({})
            if existing_vehicles:
                self.mock_db.insert_many(existing_vehicles)

    # ------------------------------------------------------------------
    # compute_rental_fee()
    # ------------------------------------------------------------------

    def test_compute_rental_fee_uses_default_tiers(self):
        """With no vehicle/subgroup, the default expression follows the base fee tiers."""
        self.assertEqual(vl.compute_rental_fee(3, None, None), 5)
        self.assertEqual(vl.compute_rental_fee(10, None, None), 35)
        self.assertEqual(vl.compute_rental_fee(48, None, None), 100)
        self.assertEqual(vl.compute_rental_fee(100, None, None), 200)
        self.assertEqual(vl.compute_rental_fee(200, None, None), 380)

    def test_compute_rental_fee_applies_discount_subgroup(self):
        """Users in the 'discount' subgroup pay half price."""
        self.assertEqual(vl.compute_rental_fee(3, 'discount', None), 2.5)
        self.assertEqual(vl.compute_rental_fee(10, 'discount', None), 17.5)

    def test_compute_rental_fee_applies_ebike_surcharge(self):
        """E_BIKE vehicles (per baseMode) cost 1.5x the base fee."""
        self.assertEqual(vl.compute_rental_fee(3, None, {'baseMode': 'E_BIKE'}), 7.5)
        self.assertEqual(vl.compute_rental_fee(3, None, {'baseMode': 'BIKE'}), 5)

    def test_compute_rental_fee_combines_discount_and_surcharge(self):
        """Discount and e-bike surcharge multiply together."""
        self.assertEqual(vl.compute_rental_fee(3, 'discount', {'baseMode': 'E_BIKE'}), 3.75)

    def test_compute_rental_fee_uses_configured_expression(self):
        """A deployment config can override the fee expression entirely."""
        fake_config = {'vehicle_library': {'fee_expression': 'duration * 2'}}
        with patch.object(vl.edc, 'get_deployment_config', return_value=fake_config):
            self.assertEqual(vl.compute_rental_fee(10, None, None), 20)

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

    def test_checkout_vehicle_cancels_hold_if_unlock_fails(self):
        """checkout_vehicle() cancels the Stripe hold when bikeep unlock raises."""
        self._insert_vehicle()

        with patch.object(vl.ss, 'create_hold_payment_intent', return_value={'id': 'pi_hold_123'}), \
             patch.object(vl.bikeep_service, 'unlock_dock', side_effect=RuntimeError('dock unreachable')), \
             patch.object(vl.ss, 'cancel_hold_payment_intent') as mock_cancel:
            with self.assertRaises(RuntimeError):
                self._checkout_vehicle()

        mock_cancel.assert_called_once_with('pi_hold_123')

    def test_checkout_vehicle_raises_original_error_when_cancel_also_fails(self):
        """checkout_vehicle() still raises the checkout error even if cancel itself fails."""
        self._insert_vehicle()

        with patch.object(vl.ss, 'create_hold_payment_intent', return_value={'id': 'pi_hold_123'}), \
             patch.object(vl.bikeep_service, 'unlock_dock', side_effect=RuntimeError('dock unreachable')), \
             patch.object(vl.ss, 'cancel_hold_payment_intent', side_effect=RuntimeError('stripe unreachable')):
            with self.assertRaisesRegex(RuntimeError, 'dock unreachable'):
                self._checkout_vehicle()

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
        # Verify new fields are populated
        self.assertIsNotNone(rental_state.get('start_local_dt'))
        self.assertIsNotNone(rental_state.get('start_fmt_time'))
        self.assertEqual(rental_state.get('start_dock_id'), DOCK_ID)
        self.assertIsNone(rental_state.get('end_local_dt'))
        self.assertIsNone(rental_state.get('end_fmt_time'))
        self.assertIsNone(rental_state.get('end_dock_id'))

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
        # Verify new end fields are populated
        self.assertIsNotNone(rental_state.get('end_local_dt'))
        self.assertIsNotNone(rental_state.get('end_fmt_time'))
        self.assertEqual(rental_state.get('end_dock_id'), ALT_DOCK_ID)

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

    def test_checkin_vehicle_resolves_scanned_code_to_device_id(self):
        """check_in_vehicle() resolves the scanned dock code to its actual Bikeep
        device id and uses the resolved id (never the scanned code) for the lock
        command - but stores/returns the code itself as Vehicle.location and
        end_dock_id/dock_id, since the client never learns the real device id."""
        self._insert_vehicle(location=str(self.test_uuid))
        self._insert_active_rental(rental_start_ts=_now())
        scanned_code = 'printed-code-123'

        with patch.object(vl.bikeep_service, 'get_device_id_for_code', return_value=ALT_DOCK_ID) as mock_resolve, \
             patch.object(vl.bikeep_service, 'lock_dock', return_value={}) as mock_lock, \
             patch.object(vl.ss, 'capture_hold_payment_intent', return_value={}):
            result = vl.check_in_vehicle(self.test_uuid, scanned_code)

        mock_resolve.assert_called_once_with(scanned_code)
        mock_lock.assert_called_once_with(ALT_DOCK_ID)
        self.assertEqual(result['dock_id'], scanned_code)
        vehicle = self.mock_db.find_one({'vehicle_id': VEHICLE_ID})
        self.assertEqual(vehicle['location'], scanned_code)

    def test_checkin_vehicle_unresolvable_code_returns_404(self):
        """check_in_vehicle() rejects a scanned code that doesn't match any device,
        without ever calling lock_dock."""
        self._insert_vehicle(location=str(self.test_uuid))
        self._insert_active_rental(rental_start_ts=_now())

        with patch.object(vl.bikeep_service, 'get_device_id_for_code', return_value=None), \
             patch.object(vl.bikeep_service, 'lock_dock') as mock_lock:
            with self.assertRaises(ValueError) as ctx:
                vl.check_in_vehicle(self.test_uuid, 'unknown-code')

        self.assertEqual(ctx.exception.args[0], 404)
        mock_lock.assert_not_called()

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

    def test_get_rental_history_returns_user_rental_entries(self):
        """get_rental_history() returns entries from manual/vehicle_rental for the user."""
        self._insert_active_rental(payment_hold_info={'id': 'pi_hold_123'}, rental_start_ts=_now())

        history = vl.get_rental_history(self.test_uuid)
        rental_history = history['rental_history']

        self.assertGreaterEqual(len(rental_history), 1)
        latest_rental = rental_history[-1]
        self.assertEqual(latest_rental['vehicle_id'], VEHICLE_ID)
        self.assertEqual(latest_rental['payment_hold_info']['id'], 'pi_hold_123')

    def test_get_rental_history_multiple_entries_latest_is_active(self):
        """When history has multiple rentals, the most recent entry can be active."""
        base_ts = _now()
        timezone = "America/Los_Angeles"
        completed_rental = ecwr.Rental({
            'vehicle_id': VEHICLE_ID,
            'vehicle_name': 'test vehicle',
            'payment_hold_info': {'id': 'pi_completed_001'},
            'start_ts': base_ts - 600,
            'start_local_dt': ecwld.LocalDate.get_local_date(base_ts - 600, timezone),
            'start_fmt_time': arrow.get(base_ts - 600).to(timezone).isoformat(),
            'start_dock_id': DOCK_ID,
            'end_ts': base_ts - 300,
            'end_local_dt': ecwld.LocalDate.get_local_date(base_ts - 300, timezone),
            'end_fmt_time': arrow.get(base_ts - 300).to(timezone).isoformat(),
            'end_dock_id': ALT_DOCK_ID,
            'rental_status': 'completed',
        })
        esta.TimeSeries.get_time_series(self.test_uuid).insert_data(
            self.test_uuid,
            vl.VEHICLE_RENTAL_KEY,
            completed_rental,
        )

        self._insert_active_rental(
            payment_hold_info={'id': 'pi_active_002'},
            rental_start_ts=base_ts,
        )

        history = vl.get_rental_history(self.test_uuid)
        rental_history = history['rental_history']

        self.assertGreaterEqual(len(rental_history), 2)
        latest_rental = rental_history[-1]
        self.assertEqual(latest_rental['rental_status'], 'active')
        self.assertEqual(latest_rental['payment_hold_info']['id'], 'pi_active_002')
        self.assertTrue(any(r['rental_status'] == 'completed' for r in rental_history[:-1]))

    # ------------------------------------------------------------------
    # _get_loc_and_timezone() / start_loc / end_loc
    # ------------------------------------------------------------------

    def test_checkout_vehicle_sets_start_loc_from_bikeep(self):
        """checkout_vehicle() sets start_loc as a GeoJSON Point using bikeep location data."""
        self._insert_vehicle()

        with patch.object(vl.ss, 'create_hold_payment_intent', return_value={'id': 'pi_hold_123'}), \
             patch.object(vl.bikeep_service, 'unlock_dock', return_value={}), \
             patch.object(vl.bikeep_service, 'get_location', return_value=_MOCK_DOCK_LOC):
            self._checkout_vehicle()

        rental_state = self._get_latest_rental_entry()['data']
        self.assertIsNotNone(rental_state.get('start_loc'))
        self.assertEqual(rental_state['start_loc']['type'], 'Point')
        self.assertEqual(rental_state['start_loc']['coordinates'], [-122.4194, 37.7749])

    def test_checkout_vehicle_uses_timezone_from_dock_location(self):
        """checkout_vehicle() derives start_fmt_time timezone from the dock's coordinates."""
        self._insert_vehicle()

        with patch.object(vl.ss, 'create_hold_payment_intent', return_value={'id': 'pi_hold_123'}), \
             patch.object(vl.bikeep_service, 'unlock_dock', return_value={}), \
             patch.object(vl.bikeep_service, 'get_location', return_value=_MOCK_DOCK_LOC):
            self._checkout_vehicle()

        rental_state = self._get_latest_rental_entry()['data']
        # SF coordinates resolve to America/Los_Angeles (UTC-7 PDT or UTC-8 PST)
        self.assertRegex(rental_state['start_fmt_time'], r'-0[78]:00$')

    def test_checkout_vehicle_falls_back_when_bikeep_location_fails(self):
        """checkout_vehicle() falls back to default timezone and None loc when location lookup fails."""
        self._insert_vehicle()

        with patch.object(vl.ss, 'create_hold_payment_intent', return_value={'id': 'pi_hold_123'}), \
             patch.object(vl.bikeep_service, 'unlock_dock', return_value={}), \
             patch.object(vl.bikeep_service, 'get_location', side_effect=RuntimeError('API down')):
            self._checkout_vehicle()  # must not raise

        rental_state = self._get_latest_rental_entry()['data']
        self.assertEqual(rental_state.get('start_loc'), {'type': 'Point', 'coordinates': [0, 0]})
        self.assertIsNotNone(rental_state.get('start_fmt_time'))

    def test_checkin_vehicle_sets_end_loc_from_bikeep(self):
        """check_in_vehicle() sets end_loc as a GeoJSON Point using bikeep location data."""
        self._insert_vehicle(location=str(self.test_uuid))
        self._insert_active_rental(rental_start_ts=_now())

        with patch.object(vl.bikeep_service, 'lock_dock', return_value={}), \
             patch.object(vl.ss, 'capture_hold_payment_intent', return_value={'status': 'succeeded'}), \
             patch.object(vl.bikeep_service, 'get_location', return_value=_MOCK_ALT_DOCK_LOC):
            vl.check_in_vehicle(self.test_uuid, ALT_DOCK_ID)

        rental_state = self._get_latest_rental_entry()['data']
        self.assertIsNotNone(rental_state.get('end_loc'))
        self.assertEqual(rental_state['end_loc']['type'], 'Point')
        self.assertEqual(rental_state['end_loc']['coordinates'], [-122.4100, 37.7900])

    def test_checkin_vehicle_falls_back_when_bikeep_location_fails(self):
        """check_in_vehicle() falls back to default timezone and None end_loc when location lookup fails."""
        self._insert_vehicle(location=str(self.test_uuid))
        self._insert_active_rental(rental_start_ts=_now())

        with patch.object(vl.bikeep_service, 'lock_dock', return_value={}), \
             patch.object(vl.ss, 'capture_hold_payment_intent', return_value={'status': 'succeeded'}), \
             patch.object(vl.bikeep_service, 'get_location', side_effect=RuntimeError('API down')):
            vl.check_in_vehicle(self.test_uuid, ALT_DOCK_ID)  # must not raise

        rental_state = self._get_latest_rental_entry()['data']
        self.assertEqual(rental_state.get('end_loc'), {'type': 'Point', 'coordinates': [0, 0]})
        self.assertIsNotNone(rental_state.get('end_fmt_time'))

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
