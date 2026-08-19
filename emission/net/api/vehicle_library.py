import logging
import time
import datetime

import emission.core.get_database as edb
import emission.core.wrapper.rental as ecwr
import emission.net.ext_service.bikeep.bikeep_service as bikeep_service
import emission.net.ext_service.stripe.stripe_service as ss
import emission.core.wrapper.payment as ecwp
import emission.storage.modifiable.abstract_state_storage as esas

logger = logging.getLogger(__name__)

DEFAULT_HOLD_AMOUNT_CENTS = 100


def _compute_rental_fee_dollars(rental_hours):
    # Keep fee tiers aligned with the client-side vehicle library computeFee().
    if 0 <= rental_hours <= 5:
        return 5
    if 5 < rental_hours <= 24:
        return 35
    if 24 < rental_hours <= 72:
        return 100
    if 72 < rental_hours <= 144:
        return 200
    return 380


def _compute_capture_amount_cents(rental_start_ts, now_ts):
    rental_hours = max(now_ts - rental_start_ts, 0) / (60 * 60)
    return _compute_rental_fee_dollars(rental_hours) * 100


def _get_rental_db(user_uuid):
    return esas.StateStorage.get_state_storage(user_uuid)


def _get_active_rental(user_uuid):
    rental_state = _get_rental_db(user_uuid).get_current_state(esas.StateName.RENTAL)
    if rental_state is None:
        return None
    if rental_state.get('rental_status') != 'active':
        return None
    return rental_state


def _upsert_rental_state(user_uuid, vehicle, payment_hold_info, now, rental_status, rental_start_ts=None):
    rental_state = ecwr.Rental()
    rental_state.vehicle_id = vehicle.get('vehicle_id')
    rental_state.vehicle_name = vehicle.get('vehicle_name')
    rental_state.payment_hold_info = payment_hold_info
    rental_state.rental_status = rental_status
    rental_state.rental_start_ts = rental_start_ts if rental_start_ts is not None else now
    rental_state.rental_end_ts = now if rental_status != 'active' else None
    _get_rental_db(user_uuid).upsert_state(esas.StateName.RENTAL, rental_state)
    return rental_state

# BEGIN: bikeeep passthrough integration
# The calls in this section are direct passthroughs to the Bikeep service.
# They allow the client to interact with Bikeep stations and docks without
# needing to know the details of the Bikeep API.

def stations():
    """
    Return a list of Bikeep station locations and dock states.
    Calls bikeep_service.get_locations() and returns the result directly.
    """
    return bikeep_service.get_locations()

# END: bikeeep passthrough integration

# BEGIN: bikeep + stripe integration
# The calls in this section handle coordination between Bikeep and Stripe services.
# They handle the reservation and checkout of vehicles, including booking
# docks via Bikeep and processing payments via Stripe.

def checkout_vehicle(user_uuid, vehicle_id, hold_amount_cents):
    """
    Check out (unlock) a vehicle for the authenticated user.

    - Places a Stripe hold using the user's saved payment method.
    - Persists the active rental mapping in the user's RENTAL state and vehicle DB.
    - Unlocks the vehicle's dock via bikeep_service.
    """
    vehicle_db = edb.get_vehicle_db()
    vehicle = vehicle_db.find_one({'vehicle_id': vehicle_id})
    if vehicle is None:
        raise ValueError(404, "Vehicle %s not found" % vehicle_id)

    now = time.time()

    dock_id = vehicle.get('location')
    if not dock_id:
        raise ValueError(422, "Vehicle %s has no dock location to unlock" % vehicle_id)

    hold_info = ss.create_hold_payment_intent(
        user_uuid,
        hold_amount_cents,
        metadata={
            'vehicle_id': vehicle_id,
            'dock_id': dock_id,
            'hold_amount_cents': hold_amount_cents,
        },
    )
    _upsert_rental_state(user_uuid, vehicle, hold_info, now, 'active')

    vehicle_db.update_one(
        {'vehicle_id': vehicle_id},
        {'$set': {
            'location': None, # TODO: Should we have this be the UUID instead?
            'checkout_ts': now,
            'updated_at': now,
        }},
    )

    logger.debug(f"Unlocking dock {dock_id} for vehicle {vehicle_id} for user {user_uuid}")
    bikeep_service.unlock_dock(dock_id)

    logger.info(f"Checked out vehicle {vehicle_id} (dock {dock_id}) for user {user_uuid}")
    return {'result': 'checked_out', 'vehicle_id': vehicle_id}


def check_in_vehicle(user_uuid, dock_id):
    """
    Check in (lock) a vehicle at the specified dock for the authenticated user.

    - Locks the specified dock via bikeep_service.
    - Captures the Stripe hold for the active rental.
    - Updates the RENTAL state and Vehicle mapping to point back to the dock.
    """
    rental_state = _get_active_rental(user_uuid)
    if rental_state is None:
        raise ValueError(403, "No vehicle is currently checked out by this user")

    vehicle_id = rental_state.get('vehicle_id')
    vehicle_db = edb.get_vehicle_db()
    vehicle = vehicle_db.find_one({'vehicle_id': vehicle_id})
    if vehicle is None:
        raise ValueError(404, "Vehicle %s not found" % vehicle_id)

    logger.debug(f"Locking dock {dock_id} for vehicle {vehicle_id} for user {user_uuid}")
    bikeep_service.lock_dock(dock_id)

    payment_hold_info = rental_state.get('payment_hold_info')
    assert payment_hold_info is not None, "Bike was rented without a hold, unsure what to capture"
    payment_hold_id = payment_hold_info.get('id')
    now = time.time()
    rental_start_ts = rental_state.get('rental_start_ts', now)
    capture_amount = _compute_capture_amount_cents(rental_start_ts, now)
    if payment_hold_id:
        ss.capture_hold_payment_intent(payment_hold_id, capture_amount)

    vehicle_db.update_one(
        {'vehicle_id': vehicle_id},
        {'$set': {
            'location': dock_id,
            'updated_at': now,
        }},
    )

    _upsert_rental_state(
        user_uuid,
        vehicle,
        payment_hold_info,
        now,
        'completed',
        rental_start_ts=rental_start_ts,
    )

    logger.info(f"Checked in vehicle {vehicle_id} to dock {dock_id} for user {user_uuid}")
    return {'result': 'checked_in', 'vehicle_id': vehicle_id, 'dock_id': dock_id}

# END: bikeep + stripe integration

## BEGIN: Stripe passthrough integration
## For the calls in this section, this module is the bridge between stripe
# internals and the communication with the client. This insulates the client
# from changes to the stripe API, and also avoids sending unncessary information
# to the client.

def initiate_user_setup(user_uuid):
    """
    Initiate the setup process for a user to enable payment and reservations.

    - Creates a Stripe setup checkout session for the user.
    - Stores the pending setup session in the user's Payment state.
    - Returns the session ID and URL to the client for redirection.
    """
    full_setup_obj = ss.create_setup_checkout_session(user_uuid)
    return {'id': full_setup_obj['id'], 'url': full_setup_obj['url']}

# Note that both check_pending_setup_status and get_user_setup_status return the
# same values; the current status of the setup process for a user.  The
# difference is that check_pending_setup_status will attempt to finalize the
# setup process by syncing with the server-side status, while
# get_user_setup_status will simply return the current status without attempting
# to finalize it. In general, we should use check_pending_setup_status only until the 
# status is SUCCEEDED or FAILED, and then use get_user_setup_status for subsequent checks.

def check_and_get_pending_setup_status(user_uuid):
    """
    Attempt to finalize the setup process by syncing with the server-side status
    Potential responses are defined in the PaymentSetupStatus enum in emission/core/wrapper/payment.py.
    """
    check_setup_status_result = ss.check_pending_setup_status(user_uuid)
    return {"payment_setup_status": str(check_setup_status_result).split(".")[-1]}  # Convert enum to string representation

def get_user_setup_status(user_uuid):
    """
    Retrieve the current setup status for a user.
    Potential responses are defined in the PaymentSetupStatus enum in emission/core/wrapper/payment.py.

    - Checks the user's Payment state for any pending setup session.
    - If a pending session exists, polls the Stripe API for its status.
    - Returns the current status of the setup process to the client.
    """
    current_payment_state = ss.get_current_payment_state(user_uuid)
    if current_payment_state is None:
        current_payment_state = ecwp.PaymentStatus.NOT_STARTED
    return {"payment_setup_status": str(current_payment_state.get("payment_setup_status", ecwp.PaymentStatus.NOT_STARTED)).split(".")[-1]}

## END: Stripe passthrough integration