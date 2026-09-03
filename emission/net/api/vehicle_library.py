import logging
import time
import datetime

import arrow
import geojson
import tzfpy

import emission.core.get_database as edb
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.rental as ecwr
import emission.core.wrapper.vehicle as ecwv
import emission.core.wrapper.localdate as ecwld
import emission.core.deployment_config as edc
import emcommon.survey.conditional_surveys as emcsc
import emission.net.ext_service.bikeep.bikeep_service as bikeep_service
import emission.net.ext_service.stripe.stripe_service as ss
import emission.core.wrapper.payment as ecwp
import emission.storage.timeseries.abstract_timeseries as esta

logger = logging.getLogger(__name__)

DEFAULT_HOLD_AMOUNT_CENTS = 100
VEHICLE_RENTAL_KEY = "manual/vehicle_rental"

def get_fee_expression():
    config = edc.get_deployment_config() or {}
    return config.get('vehicle_rental', {}).get('fee_expression')


def compute_rental_fee(duration_hours, subgroup, vehicle):
    """
    Compute the rental fee in dollars for a rental of `duration_hours`, for a
    user in `subgroup`, using `vehicle`'s own properties (baseMode, vehicle_info, ...).
    """
    config = edc.get_deployment_config() or {}
    fee_expression = config.get('vehicle_library', {}).get('fee_expression')
    # default every known vehicle field to None, so the expression can freely
    # reference e.g. `baseMode` without a NameError when it's not set/snapshotted
    scope = {prop: None for prop in ecwv.Vehicle.props}
    scope.update(vehicle or {})
    scope['duration'] = duration_hours
    scope['subgroup'] = subgroup
    return emcsc.scoped_eval(fee_expression, scope)


def _get_rental_ts(user_uuid):
    return esta.TimeSeries.get_time_series(user_uuid)


def _get_loc_and_timezone(dock_id):
    """Return (geojson.Point, timezone_str) for a dock; falls back to defaults on error."""
    try:
        loc_data = bikeep_service.get_location(dock_id)
        logger.debug(f"Retrieved location data for dock {dock_id}: {loc_data}")
        lat = loc_data["latitude"]
        lng = loc_data["longitude"]
        ret_loc = geojson.Point((lng, lat))
        ret_tz = tzfpy.get_tz(lng, lat)
        logger.debug(f"Returning location {ret_loc}, timezone {ret_tz}")
        return ret_loc, ret_tz
    except Exception as e:
        logger.exception(f"Could not look up location for dock {dock_id} because of {e}, using defaults")
        # Defaults are 0, 0 = GMT
        lat = 0
        lng = 0
        ret_loc = geojson.Point((lng, lat))
        ret_tz = tzfpy.get_tz(lng, lat)
        return ret_loc, ret_tz

def _get_active_rental_entry(user_uuid):
    active_entries = _get_rental_ts(user_uuid).find_entries(
        [VEHICLE_RENTAL_KEY],
        extra_query_list=[{"data.rental_status": "active"}],
    )
    if len(active_entries) == 0:
        return None
    return ecwe.Entry(active_entries[-1])

# BEGIN: bikeeep passthrough integration
# The calls in this section are direct passthroughs to the Bikeep service.
# They allow the client to interact with Bikeep stations and docks without
# needing to know the details of the Bikeep API.

def stations():
    """
    Return a list of Bikeep station locations and dock states, annotated with
    how many of our own fleet vehicles are actually available to rent.

    Bikeep's own `devices.available` count means "empty slots ready to accept
    a bike/locker item" - it says nothing about whether an OCCUPIED slot holds
    one of our rentable vehicles or a member of the public's personal bike.
    So for each location we fetch its devices, and count a device as holding
    a rentable vehicle only if it's LOCKED (something is secured in it) and
    our Vehicle DB has a vehicle currently parked at that device's id.
    """
    # bottle only supports returning objects, not raw lists, due to vulnerabilities with JSON arrays
    # https://stackoverflow.com/a/40695739
    locations, all_devices = bikeep_service.get_locations_and_all_devices()
    fleet_docks = _fleet_vehicle_docks()

    devices_by_location_id = {}
    for device in all_devices:
        location_id = (device.get('location') or {}).get('id')
        devices_by_location_id.setdefault(location_id, []).append(device)

    for location in locations:
        location_id = location.get('id')
        location_devices = [d for d in all_devices
                            if (d.get('location') or {}).get('id') == location_id]
        rentable_count = 0

        if location.get('connection') != 'offline':
            for device in location_devices:
                state_value = (device.get('state') or {}).get('value')
                device_code = device.get('code')
                is_fleet_dock = device_code is not None and str(device_code) in fleet_docks
                if state_value == 'LOCKED' and is_fleet_dock:
                    rentable_count += 1

        if not isinstance(location.get('devices'), dict):
            location['devices'] = {}
        location['devices']['rentable_vehicles'] = rentable_count

    logger.debug("Fetched stations: %s" % locations)
    return {'stations': locations}


def _fleet_vehicle_docks():
    """Set of dock/locker codes where one of our fleet vehicles is currently parked."""
    return {
        vehicle['location']
        for vehicle in edb.get_vehicle_db().find({'location': {'$ne': None}})
        if vehicle.get('location')
    }

# END: bikeeep passthrough integration

# BEGIN: bikeep + stripe integration
# The calls in this section handle coordination between Bikeep and Stripe services.
# They handle the reservation and checkout of vehicles, including booking
# docks via Bikeep and processing payments via Stripe.

def checkout_vehicle(user_uuid, vehicle_id, hold_amount_cents):
    """
    Check out (unlock) a vehicle for the authenticated user.

    - Places a Stripe hold using the user's saved payment method.
    - Persists the active rental mapping in the user's timeseries.
    - Unlocks the vehicle's dock via bikeep_service.
    """
    logging.debug(f"In vehicle_library module with vehicle {vehicle_id} for user {user_uuid} with hold amount {hold_amount_cents}")
    vehicle_db = edb.get_vehicle_db()
    vehicle = vehicle_db.find_one({'vehicle_id': vehicle_id})
    logging.debug(f"Found matching vehicle {vehicle=} for {vehicle_id}")
    if vehicle is None:
        raise ValueError(422, "Vehicle %s not found" % vehicle_id)

    now = time.time()

    dock_code = vehicle.get('location')
    if not dock_code:
        raise ValueError(422, "Vehicle %s has no dock location to unlock" % vehicle_id)

    dock_id = bikeep_service.get_device_id_for_code(dock_code)
    if dock_id is None:
        logging.error(f"No dock found for code {dock_code}")
        raise ValueError(422, "No dock found for code %s" % dock_code)

    start_loc, timezone = _get_loc_and_timezone(dock_id)

    hold_info = ss.create_hold_payment_intent(
        user_uuid,
        hold_amount_cents,
        metadata={
            'vehicle_id': vehicle_id,
            'dock_id': dock_id,
            'hold_amount_cents': hold_amount_cents,
        },
    )

    try:
        logger.debug(f"Unlocking dock {dock_id} (code {dock_code}) for vehicle {vehicle_id} for user {user_uuid}")
        bikeep_service.unlock_dock(dock_id)
    except Exception as e:
        logging.error(f"Error occurred while checking out vehicle {vehicle_id} for user {user_uuid}: {e}")
        try:
            ss.cancel_hold_payment_intent(hold_info.get('id'))
        except Exception as cancel_err:
            # TODO: figure out what we should do here
            logging.error(f"Failed to cancel hold {hold_info.get('id')} after checkout failure: {cancel_err}")
        raise

    # TODO: what do we do if saving the state fails here
    start_fmt_time = arrow.get(now).to(timezone).isoformat()
    start_local_dt = ecwld.LocalDate.get_local_date(now, timezone)

    rental_state = ecwr.Rental({
        'vehicle_id': vehicle.get('vehicle_id'),
        'vehicle_name': vehicle.get('vehicle_name'),
        'payment_hold_info': hold_info,
        'rental_status': 'active',
        'start_ts': now,
        'start_local_dt': start_local_dt,
        'start_fmt_time': start_fmt_time,
        'start_loc': start_loc,
        'start_dock_id': dock_code,
        'end_ts': None,
        'end_local_dt': None,
        'end_fmt_time': None,
        'end_loc': None,
        'end_dock_id': None,
    })
    _get_rental_ts(user_uuid).insert_data(user_uuid, VEHICLE_RENTAL_KEY, rental_state)

    vehicle_db.update_one(
        {'vehicle_id': vehicle_id},
        {'$set': {
            'location': None, # TODO: Should we have this be the UUID instead?
            'checkout_ts': now,
            'updated_at': now,
        }},
    )

    logger.info(f"Checked out vehicle {vehicle_id} (dock {dock_id}) for user {user_uuid}")
    return {'result': 'checked_out', 'vehicle_id': vehicle_id}


def check_in_vehicle(user_uuid, dock_code, subgroup=None):
    """
    Check in (lock) a vehicle at the dock the user scanned, for the authenticated user.

    `dock_code` is the human-readable code the user scanned/typed at the dock -
    never the real Bikeep device id, which must stay server-side - so it's
    resolved to the actual device id here before any Bikeep call is made.

    - Locks the specified dock via bikeep_service.
    - Captures the Stripe hold for the active rental.
    - Updates the active rental entry and Vehicle mapping to point back to the dock.
    """
    rental_entry = _get_active_rental_entry(user_uuid)
    if rental_entry is None:
        raise ValueError(403, "No vehicle is currently checked out by this user")
    rental_state = rental_entry.data

    vehicle_id = rental_state.vehicle_id
    vehicle_db = edb.get_vehicle_db()
    vehicle = vehicle_db.find_one({'vehicle_id': vehicle_id})
    if vehicle is None:
        raise ValueError(404, "Vehicle %s not found" % vehicle_id)

    dock_id = bikeep_service.get_device_id_for_code(dock_code)
    if dock_id is None:
        raise ValueError(404, "No dock found for code %s" % dock_code)

    logger.debug(f"Locking dock {dock_id} (code {dock_code}) for vehicle {vehicle_id} for user {user_uuid}")
    bikeep_service.lock_dock(dock_id)

    payment_hold_info = rental_state.get('payment_hold_info')
    assert payment_hold_info is not None, "Bike was rented without a hold, unsure what to capture"
    payment_hold_id = payment_hold_info.get('id')
    now = time.time()
    rental_start_ts = rental_state.start_ts
    duration_hours = max(now - rental_start_ts, 0) / (60 * 60)
    fee_dollars = compute_rental_fee(duration_hours, subgroup, rental_state.get('vehicle_info'))
    capture_amount = round(fee_dollars * 100)
    if payment_hold_id and capture_amount > 0:
        ss.capture_hold_payment_intent(payment_hold_id, capture_amount)

    vehicle_db.update_one(
        {'vehicle_id': vehicle_id},
        {'$set': {
            'location': dock_code,
            'updated_at': now,
        }},
    )

    end_loc, end_timezone = _get_loc_and_timezone(dock_id)
    updated_rental_state = ecwr.Rental({
        **rental_state,
        'rental_status': 'completed',
        'end_ts': now,
        'end_local_dt': ecwld.LocalDate.get_local_date(now, end_timezone),
        'end_fmt_time': arrow.get(now).to(end_timezone).isoformat(),
        'end_loc': end_loc,
        'end_dock_id': dock_code,
    })

    import emission.storage.timeseries.builtin_timeseries as estb
    estb.BuiltinTimeSeries.update_data(
        user_uuid,
        VEHICLE_RENTAL_KEY,
        rental_entry.get('_id'),
        updated_rental_state,
    )

    logger.info(f"Checked in vehicle {vehicle_id} to dock {dock_id} (code {dock_code}) for user {user_uuid}")
    return {'result': 'checked_in', 'vehicle_id': vehicle_id, 'dock_id': dock_code}


def get_rental_history(user_uuid):
    """Return all rental entries for the user from the vehicle rental stream."""
    # bottle only supports returning objects, not raw lists, due to vulnerabilities with JSON arrays
    # https://stackoverflow.com/a/40695739
    return {"rental_history": [e['data'] for e in _get_rental_ts(user_uuid).find_entries([VEHICLE_RENTAL_KEY])]}

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
    return {"payment_setup_status": str(check_setup_status_result).split(".")[-1],
            "is_sandbox": ss.STRIPE_IS_SANDBOX}  # Convert enum to string representation

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
    return {"payment_setup_status": str(current_payment_state.get("payment_setup_status", ecwp.PaymentStatus.NOT_STARTED)).split(".")[-1],
            "is_sandbox": ss.STRIPE_IS_SANDBOX}

## END: Stripe passthrough integration