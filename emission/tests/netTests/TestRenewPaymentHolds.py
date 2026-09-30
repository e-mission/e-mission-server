from builtins import *
import importlib.util
import os
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import arrow

os.environ.setdefault('STRIPE_SECRET_KEY', 'sk_test_dummy')

import emission.core.get_database as edb
import emission.core.wrapper.localdate as ecwld
import emission.core.wrapper.rental as ecwr
import emission.net.api.vehicle_library as vl
import emission.storage.timeseries.abstract_timeseries as esta


SCRIPT_PATH = Path(__file__).resolve().parents[3] / 'bin' / 'library' / 'renew_payment_holds.py'
_SCRIPT_SPEC = importlib.util.spec_from_file_location('renew_payment_holds', SCRIPT_PATH)
renew_payment_holds = importlib.util.module_from_spec(_SCRIPT_SPEC)
_SCRIPT_SPEC.loader.exec_module(renew_payment_holds)


class TestRenewPaymentHolds(unittest.TestCase):
    def setUp(self):
        self.profile_db = edb.get_profile_db()
        self.timeseries_db = edb.get_timeseries_db()
        self.user_ids = []

    def tearDown(self):
        if self.user_ids:
            self.profile_db.delete_many({'user_id': {'$in': self.user_ids}})
            self.timeseries_db.delete_many({
                'user_id': {'$in': self.user_ids},
                'metadata.key': vl.VEHICLE_RENTAL_KEY,
            })

    def _insert_profile(self, user_uuid, payment_hold_expires_at):
        self.user_ids.append(user_uuid)
        self.profile_db.insert_one({
            'user_id': user_uuid,
            'payment_hold_expires_at': payment_hold_expires_at,
        })

    def _insert_active_rental(self, user_uuid, payment_hold_info, vehicle_id='test-vehicle-001', rental_start_ts=None):
        rental_start_ts = rental_start_ts if rental_start_ts is not None else time.time()
        timezone = 'America/Los_Angeles'
        rental_state = ecwr.Rental({
            'vehicle_id': vehicle_id,
            'vehicle_name': 'test vehicle',
            'payment_hold_info': payment_hold_info,
            'start_ts': rental_start_ts,
            'start_local_dt': ecwld.LocalDate.get_local_date(rental_start_ts, timezone),
            'start_fmt_time': arrow.get(rental_start_ts).to(timezone).isoformat(),
            'start_dock_id': 'test-dock-1',
            'end_ts': None,
            'end_local_dt': None,
            'end_fmt_time': None,
            'end_dock_id': None,
            'rental_status': 'active',
        })
        esta.TimeSeries.get_time_series(user_uuid).insert_data(
            user_uuid,
            vl.VEHICLE_RENTAL_KEY,
            rental_state,
        )

    def test_renew_expiring_holds_counts_scanned_renewed_and_skipped(self):
        now = int(time.time())
        renewed_user = uuid.uuid4()
        future_user = uuid.uuid4()
        missing_rental_user = uuid.uuid4()
        missing_hold_user = uuid.uuid4()

        self._insert_profile(renewed_user, now + 60)
        self._insert_profile(future_user, now + (2 * renew_payment_holds.RENEWAL_WINDOW_SECS))
        self._insert_profile(missing_rental_user, now + 60)
        self._insert_profile(missing_hold_user, now + 60)

        self._insert_active_rental(
            renewed_user,
            {'id': 'pi_hold_renewed', 'amount': 100},
        )
        self._insert_active_rental(
            future_user,
            {'id': 'pi_hold_future', 'amount': 100},
        )
        self._insert_active_rental(
            missing_hold_user,
            {'amount': 100},
        )

        new_expires_at = now + 7200
        with patch.object(renew_payment_holds.ss, 'create_hold_payment_intent', return_value={
            'id': 'pi_hold_renewed_new',
            'latest_charge': {
                'payment_method_details': {
                    'card': {
                        'capture_before': new_expires_at,
                    },
                },
            },
        }), patch.object(renew_payment_holds.ss, 'cancel_hold_payment_intent', return_value={}):
            summary = renew_payment_holds.renew_expiring_holds()

        self.assertEqual(summary, {
            'scanned': 4,
            'renewed': 1,
            'skipped': 3,
            'failures': 0,
        })

        profile = self.profile_db.find_one({'user_id': renewed_user})
        self.assertEqual(profile['payment_hold_expires_at'], new_expires_at)

    def test_renew_expiring_holds_counts_failures(self):
        now = int(time.time())
        create_failure_user = uuid.uuid4()
        cancel_failure_user = uuid.uuid4()

        self._insert_profile(create_failure_user, now + 60)
        self._insert_profile(cancel_failure_user, now + 60)

        self._insert_active_rental(
            create_failure_user,
            {'id': 'pi_hold_create_fail', 'amount': 100},
        )
        self._insert_active_rental(
            cancel_failure_user,
            {'id': 'pi_hold_cancel_fail', 'amount': 100},
        )

        new_expires_at = now + 7200
        with patch.object(
            renew_payment_holds.ss,
            'create_hold_payment_intent',
            side_effect=[RuntimeError('create failed'), {
                'id': 'pi_hold_cancel_new',
                'latest_charge': {
                    'payment_method_details': {
                        'card': {
                            'capture_before': new_expires_at,
                        },
                    },
                },
            }],
        ), patch.object(
            renew_payment_holds.ss,
            'cancel_hold_payment_intent',
            side_effect=RuntimeError('cancel failed'),
        ):
            summary = renew_payment_holds.renew_expiring_holds()

        self.assertEqual(summary, {
            'scanned': 2,
            'renewed': 0,
            'skipped': 0,
            'failures': 2,
        })