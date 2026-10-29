#!/usr/bin/env python3
"""Renew Stripe payment holds that will expire within the next day.

For each user with a non-null `payment_hold_expires_ts` in the profile DB,
if the hold expires within 24 hours this script will:

1. Create a new hold for the current active rental.
2. Cancel the old hold.
3. Update the rental state and profile expiry timestamp.

The order matters: the replacement hold is placed before the old one is
cancelled so that a failed cancellation does not leave the user without a hold.
"""

from __future__ import annotations

import logging
import time
import stripe

VEHICLE_RENTAL_KEY = 'manual/vehicle_rental'

import emission.core.get_database as edb
import emission.core.wrapper.rental as ecwr
import emission.net.api.vehicle_library as vl
import emission.net.ext_service.stripe.stripe_service as ss
import emission.storage.timeseries.aggregate_timeseries as estag


RENEWAL_WINDOW_SECS = 24 * 60 * 60


def renew_hold_for_rental(rental_entry):
    user_uuid = rental_entry['user_id']
    logging.info(
        f'Forcing hold renewal for user {user_uuid}: '
        f'now={time.time()}, renewal_window_secs={RENEWAL_WINDOW_SECS}'
    )

    rental = rental_entry['data']
    capture_before_ts = lambda payment_hold_info: (
        payment_hold_info.get('latest_charge', {})
        .get('payment_method_details', {})
        .get('card', {})
        .get('capture_before')
    )

    payment_hold_info = rental.get('payment_hold_info', {})
    old_hold_id = payment_hold_info.get('id')
    hold_amount_cents = payment_hold_info.get('amount')
    payment_hold_expires_ts = capture_before_ts(payment_hold_info)

    now = time.time()
    if payment_hold_expires_ts is not None and payment_hold_expires_ts > now + RENEWAL_WINDOW_SECS:
        logging.info(f'Skipping {user_uuid}: payment hold expires beyond the renewal window')
        return False

    if not old_hold_id:
        logging.warning(f'Skipping {user_uuid}: current rental has no payment hold id')
        return False
    if hold_amount_cents is None:
        raise ValueError(f'Cannot renew hold for {user_uuid}: payment_hold_info.amount is missing')

    vehicle_id = rental.get('vehicle_id')
    logging.info(
        f'Renewing hold for user {user_uuid}, vehicle {vehicle_id}, '
        f'old_hold_id={old_hold_id}, amount_cents={hold_amount_cents}'
    )

    new_hold_info = ss.create_hold_payment_intent(
        user_uuid,
        hold_amount_cents,
        metadata={
            'vehicle_id': vehicle_id,
            'renewed_from_payment_intent_id': old_hold_id,
        },
    )

    def _cancel_new_hold():
        try:
            new_hold_id = new_hold_info.get('id')
            if new_hold_id:
                ss.cancel_hold_payment_intent(new_hold_id)
        except Exception:
            logging.exception(f'Failed to cancel replacement hold for user {user_uuid}')

    try:
        ss.cancel_hold_payment_intent(old_hold_id)
    except stripe._error.InvalidRequestError as ire:
        if ire.code == 'payment_intent_unexpected_state':
            old_hold_info = ss.stripe.PaymentIntent.retrieve(old_hold_id)
            logging.debug(f"old_hold_info = {old_hold_info} with type {type(old_hold_info)}")
            old_hold_status = old_hold_info['status']
            if old_hold_status == 'canceled':
                logging.info(f'Old hold {old_hold_id} for user {user_uuid} is already canceled')
            else:
                logging.error(f'Old hold {old_hold_id} for user {user_uuid} is in unexpected state: {old_hold_status} and cannot be cancelled')
                _cancel_new_hold()
        else:
            logging.error(f'Received unexpected InvalidRequestError while canceling old hold {old_hold_id} for user {user_uuid}: {ire}')
            _cancel_new_hold()
    except Exception:
        logging.exception(f'Failed to cancel old hold {old_hold_id} for user {user_uuid}')
        _cancel_new_hold()

    rental['payment_hold_info'] = new_hold_info
    vl._update_rental_state(user_uuid, rental_entry['_id'], rental)

    # We don't actually use the `new_expires_at` any more but it is good to show it in the dashboard
    new_expires_at = capture_before_ts(new_hold_info)

    edb.get_profile_db().update_one(
        {'user_id': user_uuid},
        {'$set': {'payment_hold_expires_ts': new_expires_at}},
        upsert=True,
    )
    logging.info(
        f'Renewed hold for user {user_uuid}, '
        f'new_hold_id={new_hold_info.get("id")}, expires_at={new_expires_at}'
    )
    return True

def renew_expiring_holds():
    aggregate_ts = estag.AggregateTimeSeries()
    all_active_rentals = aggregate_ts.find_entries(
        [VEHICLE_RENTAL_KEY],
        extra_query_list=[
            {"data.rental_status": {"$in": [ecwr.RentalStatus.ACTIVE, ecwr.RentalStatus.INITIALIZING]}}
        ],
    )

    scanned = 0
    renewed = 0
    skipped = 0
    failures = 0

    for rental in all_active_rentals:
        scanned += 1
        user_uuid = rental.get('user_id')
        logging.debug(f"Evaluating payment hold for user {rental['user_id']} and rental with state {rental['data']['rental_status']} starting at {rental['data']['start_fmt_time']}")
        try:
            if renew_hold_for_rental(rental):
                renewed += 1
                logging.info(f"Renewed payment hold for user {user_uuid}")
            else:
                skipped += 1
                logging.debug(f"Skipped payment hold renewal for user {user_uuid}")
        except Exception:
            failures += 1
            logging.exception(f'Failed to renew hold for user {user_uuid}')

    logging.info(
        f"Finished scanning payment holds: scanned={scanned}, renewed={renewed}, "
        f"skipped={skipped}, failures={failures}"
    )
    return {
        'scanned': scanned,
        'renewed': renewed,
        'skipped': skipped,
        'failures': failures,
    }


def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
    summary = renew_expiring_holds()
    logging.info(
        f"Renewal summary: scanned={summary['scanned']}, renewed={summary['renewed']}, "
        f"skipped={summary['skipped']}, failures={summary['failures']}"
    )
    print(
        'Renewal summary: '
        f"scanned={summary['scanned']}, renewed={summary['renewed']}, "
        f"skipped={summary['skipped']}, failures={summary['failures']}"
    )
    if summary['failures']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
