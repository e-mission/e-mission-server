#!/usr/bin/env python3
"""Seed fake rental history for a user into the timeseries.

Inserts `num_completed` completed rentals followed by one active rental,
each spaced `interval_secs` apart. Useful for manual testing and dev.
"""

from __future__ import annotations

import argparse
import time
import uuid

import emission.core.wrapper.rental as ecwr
import emission.storage.timeseries.abstract_timeseries as esta

VEHICLE_RENTAL_KEY = "manual/vehicle_rental"


def seed_rental_history(
    user_uuid: uuid.UUID,
    vehicle_id: str = "test-bike-001",
    num_completed: int = 3,
    interval_secs: float = 3600,
) -> None:
    base_ts = time.time() - (num_completed * interval_secs)
    ts = esta.TimeSeries.get_time_series(user_uuid)

    for i in range(num_completed):
        start = base_ts + i * interval_secs
        ts.insert_data(user_uuid, VEHICLE_RENTAL_KEY, ecwr.Rental({
            'vehicle_id': vehicle_id,
            'vehicle_name': vehicle_id,
            'payment_hold_info': {'id': f'pi_completed_{i:03d}'},
            'rental_status': 'completed',
            'start_ts': start,
            'end_ts': start + interval_secs * 0.9,
        }))

    active_start = base_ts + num_completed * interval_secs
    ts.insert_data(user_uuid, VEHICLE_RENTAL_KEY, ecwr.Rental({
        'vehicle_id': vehicle_id,
        'vehicle_name': vehicle_id,
        'payment_hold_info': {'id': 'pi_active_latest'},
        'rental_status': 'active',
        'start_ts': active_start,
        'end_ts': None,
    }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("user_uuid", type=uuid.UUID, help="UUID of the user to seed rentals for")
    parser.add_argument("--vehicle-id", default="test-bike-001")
    parser.add_argument("--num-completed", type=int, default=3)
    parser.add_argument("--interval-secs", type=float, default=3600)
    args = parser.parse_args()

    seed_rental_history(
        args.user_uuid,
        vehicle_id=args.vehicle_id,
        num_completed=args.num_completed,
        interval_secs=args.interval_secs,
    )
    print(f"Seeded {args.num_completed} completed + 1 active rental(s) for {args.user_uuid}")
