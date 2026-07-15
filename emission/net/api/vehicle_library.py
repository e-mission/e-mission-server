import logging
import time
import datetime

from emission.net.api.bottle import request, HTTPError
import emission.core.get_database as edb
import emission.net.ext_service.bikeep.bikeep_service as bikeep_service

logger = logging.getLogger(__name__)


def stations():
    """
    Return a list of Bikeep station locations and dock states.
    Calls bikeep_service.get_locations() and returns the result directly.
    """
    return bikeep_service.get_locations()


def reserve_vehicle(user_uuid):
    """
    Reserve a vehicle for the authenticated user.

    Expects JSON body: {"vehicle_id": "<vehicle_id>"}

    - Looks up the vehicle in the DB.
    - Fails with 409 if the vehicle already has an active (unexpired) reservation.
    - Books the vehicle's current dock via bikeep_service with a 1-hour timeout.
    - Persists the reservation in the Vehicle document.
    """
    vehicle_id = request.json.get('vehicle_id')
    if not vehicle_id:
        raise HTTPError(400, "vehicle_id is required")

    vehicle_db = edb.get_vehicle_db()
    vehicle = vehicle_db.find_one({'vehicle_id': vehicle_id})
    if vehicle is None:
        raise HTTPError(404, "Vehicle %s not found" % vehicle_id)

    now = time.time()
    existing = vehicle.get('reservation')
    if existing and existing.get('expires_ts') is not None and existing['expires_ts'] > now:
        raise HTTPError(409, "Vehicle %s already has an active reservation" % vehicle_id)

    expires_ts = now + 3600
    expires_iso = datetime.datetime.utcfromtimestamp(expires_ts).strftime('%Y-%m-%dT%H:%M:%SZ')

    dock_id = vehicle.get('location')
    if not dock_id:
        raise HTTPError(422, "Vehicle %s has no location; cannot reserve" % vehicle_id)

    logger.debug("Booking dock %s for vehicle %s for user %s" % (dock_id, vehicle_id, user_uuid))
    bikeep_service.book_dock(dock_id, timeout_at=expires_iso)

    reservation = {
        'user_uuid': str(user_uuid),
        'expires_ts': expires_ts,
        'checkout_ts': None,
        'original_dock_id': dock_id,
        'charge_id': None,
    }
    vehicle_db.update_one(
        {'vehicle_id': vehicle_id},
        {'$set': {'reservation': reservation, 'updated_at': now}},
    )

    logger.info("Reserved vehicle %s (dock %s) for user %s, expires %s" % (
        vehicle_id, dock_id, user_uuid, expires_iso))
    return {'result': 'reserved', 'vehicle_id': vehicle_id, 'expires_ts': expires_ts}


def checkout_vehicle(user_uuid):
    """
    Check out (unlock) a reserved vehicle for the authenticated user.

    Expects JSON body: {"vehicle_id": "<vehicle_id>"}

    - Validates that the vehicle has an active reservation belonging to this user.
    - Unlocks the vehicle's dock via bikeep_service.
    - Updates the Vehicle location to the user's UUID and records checkout_ts.
    """
    vehicle_id = request.json.get('vehicle_id')
    if not vehicle_id:
        raise HTTPError(400, "vehicle_id is required")

    vehicle_db = edb.get_vehicle_db()
    vehicle = vehicle_db.find_one({'vehicle_id': vehicle_id})
    if vehicle is None:
        raise HTTPError(404, "Vehicle %s not found" % vehicle_id)

    now = time.time()
    reservation = vehicle.get('reservation')
    if not reservation:
        raise HTTPError(403, "Vehicle %s has no reservation" % vehicle_id)
    if reservation.get('user_uuid') != str(user_uuid):
        raise HTTPError(403, "Vehicle %s is not reserved by this user" % vehicle_id)
    if reservation.get('expires_ts') is None or reservation['expires_ts'] <= now:
        raise HTTPError(403, "Reservation for vehicle %s has expired" % vehicle_id)

    dock_id = vehicle.get('location')
    if not dock_id:
        raise HTTPError(422, "Vehicle %s has no dock location to unlock" % vehicle_id)

    logger.debug("Unlocking dock %s for vehicle %s for user %s" % (dock_id, vehicle_id, user_uuid))
    bikeep_service.unlock_dock(dock_id)

    vehicle_db.update_one(
        {'vehicle_id': vehicle_id},
        {'$set': {
            'location': str(user_uuid),
            'reservation.checkout_ts': now,
            'updated_at': now,
        }},
    )

    logger.info("Checked out vehicle %s (dock %s) for user %s" % (vehicle_id, dock_id, user_uuid))
    return {'result': 'checked_out', 'vehicle_id': vehicle_id}


def check_in_vehicle(user_uuid):
    """
    Check in (lock) a vehicle at the specified dock for the authenticated user.

    Expects JSON body: {"dock_id": "<dock_id>"}

    - Finds the vehicle currently checked out by this user (location == str(user_uuid)).
    - Fails with 403 if the user has no vehicle checked out.
    - Locks the specified dock via bikeep_service.
    - Updates the Vehicle location to the dock_id and clears the reservation.
    """
    dock_id = request.json.get('dock_id')
    if not dock_id:
        raise HTTPError(400, "dock_id is required")

    vehicle_db = edb.get_vehicle_db()
    vehicle = vehicle_db.find_one({'location': str(user_uuid)})
    if vehicle is None:
        raise HTTPError(403, "No vehicle is currently checked out by this user")

    vehicle_id = vehicle.get('vehicle_id')
    logger.debug("Locking dock %s for vehicle %s for user %s" % (dock_id, vehicle_id, user_uuid))
    bikeep_service.lock_dock(dock_id)

    now = time.time()
    vehicle_db.update_one(
        {'vehicle_id': vehicle_id},
        {'$set': {
            'location': dock_id,
            'reservation': None,
            'updated_at': now,
        }},
    )

    logger.info("Checked in vehicle %s to dock %s for user %s" % (vehicle_id, dock_id, user_uuid))
    return {'result': 'checked_in', 'vehicle_id': vehicle_id, 'dock_id': dock_id}
