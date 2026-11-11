#!/usr/bin/env python3
"""Sync vehicles from a JSON file into vehicle_db.

Sync behavior:
- Insert vehicles present in JSON but not in vehicle_db.
- Delete vehicles present in vehicle_db but not in JSON.

Existing vehicles that are present in both places are left unchanged.
"""

from __future__ import annotations

import argparse
import copy
import json
import time
from typing import Dict, List, Tuple


JsonVehicle = Dict[str, object]


def _load_vehicle_list(json_path: str) -> List[JsonVehicle]:
    with open(json_path) as fp:
        raw = json.load(fp)

    if not isinstance(raw, dict):
        raise ValueError("Input JSON must be an object with a 'vehicle_identities' list")

    vehicles = raw.get("vehicle_identities")
    if not isinstance(vehicles, list):
        raise ValueError("Input JSON must include 'vehicle_identities' as a list")

    parsed: List[JsonVehicle] = []
    seen_ids = set()

    for idx, vehicle in enumerate(vehicles):
        if not isinstance(vehicle, dict):
            raise ValueError(f"Vehicle at index {idx} is not an object")

        vehicle_id = vehicle.get("vehicle_id")
        if not isinstance(vehicle_id, str) or len(vehicle_id.strip()) == 0:
            raise ValueError(f"Vehicle at index {idx} is missing a non-empty 'vehicle_id'")

        if vehicle_id in seen_ids:
            raise ValueError(f"Duplicate vehicle_id in JSON: {vehicle_id}")
        seen_ids.add(vehicle_id)

        parsed.append(vehicle)

    return parsed


def _compute_sync_sets(
    json_vehicles: List[JsonVehicle],
    db_vehicles: List[JsonVehicle],
) -> Tuple[List[JsonVehicle], List[str]]:
    json_by_id = {v["vehicle_id"]: v for v in json_vehicles}
    db_ids = {
        d.get("vehicle_id")
        for d in db_vehicles
        if isinstance(d.get("vehicle_id"), str) and len(d.get("vehicle_id")) > 0
    }

    json_ids = set(json_by_id.keys())

    to_insert_ids = sorted(json_ids - db_ids)
    to_delete_ids = sorted(db_ids - json_ids)

    now = time.time()
    to_insert: List[JsonVehicle] = []
    for vehicle_id in to_insert_ids:
        vehicle_doc = copy.deepcopy(json_by_id[vehicle_id])
        vehicle_doc.setdefault("created_at", now)
        vehicle_doc["updated_at"] = now
        to_insert.append(vehicle_doc)

    return to_insert, to_delete_ids


def sync_vehicles(json_path: str, dry_run: bool = False) -> None:
    import emission.core.get_database as edb

    json_vehicles = _load_vehicle_list(json_path)
    vehicle_db = edb.get_vehicle_db()

    db_vehicles = list(vehicle_db.find({}, {"vehicle_id": 1}))
    to_insert, to_delete_ids = _compute_sync_sets(json_vehicles, db_vehicles)

    print(f"Loaded {len(json_vehicles)} vehicles from JSON")
    print(f"Found {len(db_vehicles)} vehicles in vehicle_db")
    print(f"Will insert {len(to_insert)} vehicles")
    print(f"Will delete {len(to_delete_ids)} vehicles")

    if len(to_insert) > 0:
        print("Insert vehicle_ids:", [v["vehicle_id"] for v in to_insert])
    if len(to_delete_ids) > 0:
        print("Delete vehicle_ids:", to_delete_ids)

    if dry_run:
        print("Dry run enabled; no changes applied")
        return

    if len(to_insert) > 0:
        vehicle_db.insert_many(to_insert)

    if len(to_delete_ids) > 0:
        vehicle_db.delete_many({"vehicle_id": {"$in": to_delete_ids}})

    print("Sync complete")


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync vehicles from JSON into vehicle_db",
    )
    parser.add_argument(
        "json_file",
        help="Path to JSON file containing {'vehicle_identities': [...]} vehicle objects",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show insert/delete actions without writing to the database",
    )
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    sync_vehicles(args.json_file, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
