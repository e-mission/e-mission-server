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

import emission.core.get_database as edb
import emission.core.wrapper.rental as ecwr
import emission.net.api.vehicle_library as vl
import emission.net.ext_service.stripe.stripe_service as ss


RENEWAL_WINDOW_SECS = 24 * 60 * 60


def renew_hold_for_user(user_uuid):
    profile = edb.get_profile_db().find_one({'user_id': user_uuid})
    if profile is None:
        logging.warning(f'Skipping {user_uuid}: no profile found')
        return False

    payment_hold_expires_ts = profile.get('payment_hold_expires_ts')
    if payment_hold_expires_ts is None:
        return False

    now = time.time()
    if payment_hold_expires_ts > now + RENEWAL_WINDOW_SECS:
        return False

    rental_entry = vl._get_active_rental_entry(user_uuid)
    if rental_entry is None:
        logging.warning(f'Skipping {user_uuid}: no active rental entry found')
        return False

    rental_state = rental_entry.data

    payment_hold_info = rental_state.get('payment_hold_info') or {}
    old_hold_id = payment_hold_info.get('id')
    hold_amount_cents = payment_hold_info.get('amount')

    if not old_hold_id:
        logging.warning(f'Skipping {user_uuid}: current rental has no payment hold id')
        return False
    if hold_amount_cents is None:
        raise ValueError(f'Cannot renew hold for {user_uuid}: payment_hold_info.amount is missing')

    vehicle_id = rental_state.get('vehicle_id')
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

    try:
        ss.cancel_hold_payment_intent(old_hold_id)
    except Exception:
        logging.exception(f'Failed to cancel old hold {old_hold_id} for user {user_uuid}')
        try:
            new_hold_id = new_hold_info.get('id')
            if new_hold_id:
                ss.cancel_hold_payment_intent(new_hold_id)
        except Exception:
            logging.exception(f'Failed to cancel replacement hold for user {user_uuid}')
        raise

    new_expires_at = (
        new_hold_info.get('latest_charge', {})
        .get('payment_method_details', {})
        .get('card', {})
        .get('capture_before')
    )
    if new_expires_at is None:
        raise ValueError(f'New hold for {user_uuid} did not include capture_before')

    rental_state['payment_hold_info'] = new_hold_info
    vl._update_rental_state(user_uuid, rental_entry['_id'], rental_state)
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
    profile_db = edb.get_profile_db()
    due_profiles = profile_db.find({
        'payment_hold_expires_ts': {
            '$ne': None,
            '$exists': True,
        }
    })

    scanned = 0
    renewed = 0
    skipped = 0
    failures = 0

    for profile in due_profiles:
        scanned += 1
        user_uuid = profile.get('user_id')
        if user_uuid is None:
            skipped += 1
            continue

        try:
            if renew_hold_for_user(user_uuid):
                renewed += 1
            else:
                skipped += 1
        except Exception:
            failures += 1
            logging.exception(f'Failed to renew hold for user {user_uuid}')

    return {
        'scanned': scanned,
        'renewed': renewed,
        'skipped': skipped,
        'failures': failures,
    }


def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
    summary = renew_expiring_holds()
    print(
        'Renewal summary: '
        f"scanned={summary['scanned']}, renewed={summary['renewed']}, "
        f"skipped={summary['skipped']}, failures={summary['failures']}"
    )
    if summary['failures']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()