from builtins import *
import os
import time
import unittest
import uuid
from unittest.mock import patch

import arrow

import emission.core.get_database as edb
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.localdate as ecwld
import emission.core.wrapper.rental as ecwr
import emission.net.api.vehicle_library as vl
import emission.storage.timeseries.abstract_timeseries as esta

os.environ.setdefault('STRIPE_SECRET_KEY', 'sk_test_dummy')

VEHICLE_ID = "test-bike-fsm-001"
DOCK_ID = "test-dock-fsm-1"
ALT_DOCK_ID = "test-dock-fsm-2"


def _now():
    return time.time()


class TestVehicleLibraryFSM(unittest.TestCase):
    def setUp(self):
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

        self.mock_db.delete_many({'vehicle_id': VEHICLE_ID})
        self.mock_db.delete_many({'location': str(self.test_uuid)})
        self.profile_db.delete_many({'user_id': self.test_uuid})
        self.state_db.delete_many({'user_id': self.test_uuid})
        self.timeseries_db.delete_many({'user_id': self.test_uuid, 'metadata.key': vl.VEHICLE_RENTAL_KEY})

    def tearDown(self):
        self.mock_db.delete_many({'vehicle_id': VEHICLE_ID})
        self.mock_db.delete_many({'location': str(self.test_uuid)})
        self.profile_db.delete_many({'user_id': self.test_uuid})
        self.state_db.delete_many({'user_id': self.test_uuid})
        self.timeseries_db.delete_many({'user_id': self.test_uuid, 'metadata.key': vl.VEHICLE_RENTAL_KEY})
        self._dock_code_patcher.stop()
        self._fee_config_patcher.stop()
        self._sandbox_patcher.stop()

    def _insert_vehicle(self, location=DOCK_ID):
        now = _now()
        doc = {
            'vehicle_id': VEHICLE_ID,
            'location': location,
            'created_at': now,
            'updated_at': now,
        }
        self.mock_db.insert_one(doc)
        return doc

    def _insert_rental(self, status, payment_hold_info=None, start_ts=None, end_ts=None):
        start_ts = start_ts if start_ts is not None else _now()
        timezone = "America/Los_Angeles"
        rental_state = ecwr.Rental({
            'vehicle_id': VEHICLE_ID,
            'vehicle_name': 'fsm vehicle',
            'payment_hold_info': payment_hold_info,
            'start_ts': start_ts,
            'start_local_dt': ecwld.LocalDate.get_local_date(start_ts, timezone),
            'start_fmt_time': arrow.get(start_ts).to(timezone).isoformat(),
            'start_dock_id': DOCK_ID,
            'start_loc': None,
            'end_ts': end_ts,
            'end_local_dt': ecwld.LocalDate.get_local_date(end_ts, timezone) if end_ts is not None else None,
            'end_fmt_time': arrow.get(end_ts).to(timezone).isoformat() if end_ts is not None else None,
            'end_dock_id': ALT_DOCK_ID if end_ts is not None else None,
            'end_loc': None,
            'rental_status': status,
        })
        esta.TimeSeries.get_time_series(self.test_uuid).insert_data(
            self.test_uuid,
            vl.VEHICLE_RENTAL_KEY,
            rental_state,
        )
        return rental_state

    def _latest_rental_entry(self):
        entries = esta.TimeSeries.get_time_series(self.test_uuid).find_entries([vl.VEHICLE_RENTAL_KEY])
        return None if len(entries) == 0 else ecwe.Entry(entries[-1])

    def _latest_rental_status(self):
        return self._latest_rental_entry().data.rental_status

    def _checkout_vehicle(self, hold_amount_cents=vl.DEFAULT_HOLD_AMOUNT_CENTS):
        return vl.checkout_vehicle(self.test_uuid, VEHICLE_ID, hold_amount_cents)

    def _recorded_update_statuses(self):
        recorded_statuses = []
        original_update = vl._update_rental_state

        def record_and_update(user_uuid, rental_entry_id, new_rental_state):
            recorded_statuses.append(new_rental_state['rental_status'])
            return original_update(user_uuid, rental_entry_id, new_rental_state)

        return recorded_statuses, record_and_update

    def test_fsm_edge_new_bike_to_initializing(self):
        self._insert_vehicle(location='UNINITIALIZED')

        with patch.object(vl.ss, 'create_hold_payment_intent') as mock_hold, \
             patch.object(vl.bikeep_service, 'unlock_dock') as mock_unlock:
            result = self._checkout_vehicle()

        self.assertEqual(result['result'], ecwr.RentalStatus.INITIALIZING)
        self.assertEqual(self._latest_rental_status(), 'initializing')
        mock_hold.assert_not_called()
        mock_unlock.assert_not_called()

    def test_fsm_edge_initializing_to_completed(self):
        self._insert_vehicle(location='UNINITIALIZED')
        self._insert_rental(ecwr.RentalStatus.INITIALIZING, payment_hold_info=None)

        with patch.object(vl.ss, 'capture_hold_payment_intent') as mock_capture, \
             patch.object(vl.bikeep_service, 'lock_dock', return_value={}) as mock_lock:
            result = vl.check_in_vehicle(self.test_uuid, ALT_DOCK_ID)

        self.assertEqual(result['result'], 'checked_in')
        self.assertEqual(self._latest_rental_status(), 'completed')
        mock_capture.assert_not_called()
        mock_lock.assert_called_once_with(ALT_DOCK_ID)

    def test_fsm_edge_new_user_to_started(self):
        self._insert_vehicle()
        recorded_statuses, record_and_update = self._recorded_update_statuses()

        with patch.object(vl, '_update_rental_state', side_effect=record_and_update), \
             patch.object(vl.ss, 'create_hold_payment_intent', side_effect=ValueError(402, 'hold failed')):
            with self.assertRaises(ValueError):
                self._checkout_vehicle()

        self.assertGreaterEqual(len(recorded_statuses), 1)
        self.assertEqual(recorded_statuses[0], ecwr.RentalStatus.STARTED)
        self.assertEqual(self._latest_rental_status(), 'cancelled')

    def test_fsm_edge_completed_to_started(self):
        self._insert_vehicle()
        self._insert_rental(ecwr.RentalStatus.COMPLETED, payment_hold_info={'id': 'pi_completed'}, end_ts=_now())
        recorded_statuses, record_and_update = self._recorded_update_statuses()

        with patch.object(vl, '_update_rental_state', side_effect=record_and_update), \
             patch.object(vl.ss, 'create_hold_payment_intent', side_effect=ValueError(402, 'hold failed')):
            with self.assertRaises(ValueError):
                self._checkout_vehicle()

        self.assertGreaterEqual(len(recorded_statuses), 1)
        self.assertEqual(recorded_statuses[0], ecwr.RentalStatus.STARTED)
        self.assertEqual(self._latest_rental_status(), 'cancelled')

    def test_fsm_edges_started_to_held_to_active(self):
        self._insert_vehicle()
        recorded_statuses, record_and_update = self._recorded_update_statuses()

        with patch.object(vl, '_update_rental_state', side_effect=record_and_update), \
             patch.object(vl.ss, 'create_hold_payment_intent', return_value={'id': 'pi_hold_123'}), \
             patch.object(vl.bikeep_service, 'unlock_dock', return_value={}):
            result = self._checkout_vehicle()

        self.assertEqual(result['result'], ecwr.RentalStatus.ACTIVE)
        self.assertEqual(recorded_statuses[:3], [
            ecwr.RentalStatus.STARTED,
            ecwr.RentalStatus.HELD,
            ecwr.RentalStatus.ACTIVE,
        ])
        self.assertEqual(self._latest_rental_status(), 'active')

    def test_fsm_edge_started_to_cancelled_on_hold_failure(self):
        self._insert_vehicle()
        recorded_statuses, record_and_update = self._recorded_update_statuses()

        with patch.object(vl, '_update_rental_state', side_effect=record_and_update), \
             patch.object(vl.ss, 'create_hold_payment_intent', side_effect=ValueError(402, 'hold failed')), \
             patch.object(vl.bikeep_service, 'unlock_dock') as mock_unlock:
            with self.assertRaises(ValueError) as ctx:
                self._checkout_vehicle()

        self.assertEqual(ctx.exception.args[0], 422)
        self.assertEqual(recorded_statuses[:2], [
            ecwr.RentalStatus.STARTED,
            ecwr.RentalStatus.CANCELLED,
        ])
        self.assertEqual(self._latest_rental_status(), 'cancelled')
        mock_unlock.assert_not_called()

    def test_fsm_edge_held_to_cancelled_on_unlock_failure_with_cancel_success(self):
        self._insert_vehicle()
        recorded_statuses, record_and_update = self._recorded_update_statuses()

        with patch.object(vl, '_update_rental_state', side_effect=record_and_update), \
             patch.object(vl.ss, 'create_hold_payment_intent', return_value={'id': 'pi_hold_123'}), \
             patch.object(vl.bikeep_service, 'unlock_dock', side_effect=RuntimeError('unlock failed')), \
             patch.object(vl.ss, 'cancel_hold_payment_intent', return_value={'id': 'pi_hold_123', 'status': 'canceled'}) as mock_cancel:
            with self.assertRaises(ValueError) as ctx:
                self._checkout_vehicle()

        self.assertEqual(ctx.exception.args[0], 424)
        self.assertEqual(recorded_statuses[:3], [
            ecwr.RentalStatus.STARTED,
            ecwr.RentalStatus.HELD,
            ecwr.RentalStatus.CANCELLED,
        ])
        self.assertEqual(self._latest_rental_status(), 'cancelled')
        mock_cancel.assert_called_once_with('pi_hold_123')

    def test_fsm_edge_held_to_held_on_unlock_failure_with_cancel_failure(self):
        self._insert_vehicle()
        recorded_statuses, record_and_update = self._recorded_update_statuses()

        with patch.object(vl, '_update_rental_state', side_effect=record_and_update), \
             patch.object(vl.ss, 'create_hold_payment_intent', return_value={'id': 'pi_hold_123'}), \
             patch.object(vl.bikeep_service, 'unlock_dock', side_effect=RuntimeError('unlock failed')), \
             patch.object(vl.ss, 'cancel_hold_payment_intent', side_effect=RuntimeError('cancel failed')):
            with self.assertRaises(ValueError) as ctx:
                self._checkout_vehicle()

        self.assertEqual(ctx.exception.args[0], 424)
        self.assertEqual(recorded_statuses[:2], [
            ecwr.RentalStatus.STARTED,
            ecwr.RentalStatus.HELD,
        ])
        self.assertEqual(self._latest_rental_status(), 'held')

    def test_fsm_edges_active_to_captured_to_completed(self):
        self._insert_vehicle(location=str(self.test_uuid))
        self._insert_rental(ecwr.RentalStatus.ACTIVE, payment_hold_info={'id': 'pi_hold_123'})
        recorded_statuses, record_and_update = self._recorded_update_statuses()

        with patch.object(vl, '_update_rental_state', side_effect=record_and_update), \
             patch.object(vl.ss, 'capture_hold_payment_intent', return_value={'id': 'pi_hold_123', 'status': 'succeeded'}), \
             patch.object(vl.bikeep_service, 'lock_dock', return_value={}):
            result = vl.check_in_vehicle(self.test_uuid, ALT_DOCK_ID)

        self.assertEqual(result['result'], 'checked_in')
        self.assertEqual(recorded_statuses, [
            ecwr.RentalStatus.CAPTURED,
            ecwr.RentalStatus.COMPLETED,
        ])
        self.assertEqual(self._latest_rental_status(), 'completed')

    def test_fsm_edge_active_to_active_on_capture_failure(self):
        self._insert_vehicle(location=str(self.test_uuid))
        self._insert_rental(ecwr.RentalStatus.ACTIVE, payment_hold_info={'id': 'pi_hold_123'})

        with patch.object(vl.ss, 'capture_hold_payment_intent', side_effect=ValueError(424, 'capture failed')), \
             patch.object(vl.bikeep_service, 'lock_dock') as mock_lock:
            with self.assertRaises(ValueError) as ctx:
                vl.check_in_vehicle(self.test_uuid, ALT_DOCK_ID)

        self.assertEqual(ctx.exception.args[0], 424)
        self.assertEqual(self._latest_rental_status(), 'active')
        mock_lock.assert_not_called()

    def test_fsm_edge_captured_to_captured_on_lock_failure(self):
        self._insert_vehicle(location=str(self.test_uuid))
        self._insert_rental(ecwr.RentalStatus.ACTIVE, payment_hold_info={'id': 'pi_hold_123'})
        recorded_statuses, record_and_update = self._recorded_update_statuses()

        with patch.object(vl, '_update_rental_state', side_effect=record_and_update), \
             patch.object(vl.ss, 'capture_hold_payment_intent', return_value={'id': 'pi_hold_123', 'status': 'succeeded'}), \
             patch.object(vl.bikeep_service, 'lock_dock', side_effect=RuntimeError('lock failed')):
            with self.assertRaises(ValueError) as ctx:
                vl.check_in_vehicle(self.test_uuid, ALT_DOCK_ID)

        self.assertEqual(ctx.exception.args[0], 424)
        self.assertEqual(recorded_statuses, [ecwr.RentalStatus.CAPTURED])
        self.assertEqual(self._latest_rental_status(), 'captured')