import json
import os
import logging
import stripe
import emission.storage.modifiable.abstract_state_storage as esas
import emission.core.wrapper.payment as ecwp

logging.debug(f"About to configure the stripe service")

# BEGIN DO NOT REFACTOR: I do not want to wrap these accesses
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY")
APP_URL_PREFIX = os.environ.get("APP_URL_PREFIX")
STRIPE_SUCCESS_URL = APP_URL_PREFIX + "payment/setup/success" if APP_URL_PREFIX else None
STRIPE_CANCEL_URL = APP_URL_PREFIX + "payment/setup/cancel" if APP_URL_PREFIX else None
STRIPE_API_BASE = "https://api.stripe.com"
# END DO NOT REFACTOR: I do not want to wrap these accesses

logging.debug("About to configure stripe with STRIPE_SECRET_KEY=%s,\
               STRIPE_API_BASE=%s" % (
    STRIPE_SECRET_KEY[:5] + "..." if STRIPE_SECRET_KEY else None,
    STRIPE_API_BASE,
))
stripe.api_key = STRIPE_SECRET_KEY
stripe.api_base = STRIPE_API_BASE

logging.debug(f"Finished configuring the stripe service with {STRIPE_SUCCESS_URL=}")

# BEGIN: Setup flow; checkout session with a "setup" mode, and a hosted URL.
# This requires a call to set up the session, which returns the hosted URL to the phone
# a call that is initiated by the phone once the user has completed the setup on the stripe site,
# and a call to poll for the current session status, so that the phone can handle edge conditions
# related to cancellation and not opening the app success URL

def create_setup_checkout_session(uuid):
    logging.debug("Creating stripe setup checkout session for user %s" % uuid)
    payment_db = esas.StateStorage.get_state_storage(uuid)
    payment_info = payment_db.get_current_state(esas.StateName.PAYMENT)
    if payment_info is None:
        logging.debug(f"DEBUG: No current payment session found for user {uuid}, creating a new session")
        curr_payment_session = ecwp.Payment({"payment_setup_status": ecwp.PaymentSetupStatus.NOT_STARTED})
    else:
        logging.debug(f"DEBUG: Found current payment session for user {uuid}: {payment_info}")
        curr_payment_session = payment_info
    curr_payment_setup_status = curr_payment_session.payment_setup_status
    logging.debug(f"DEBUG: Current payment setup status for user {uuid}: {curr_payment_setup_status}")

    # If there is an existing pending session, use it. Otherwise create a new one.
    if curr_payment_setup_status == ecwp.PaymentSetupStatus.WAITING_FOR_USER:
        logging.debug(f"DEBUG: Current payment setup status is WAITING_FOR_USER for user {uuid}, checking session status")
        # We get this from the API instead of the database because the database does not store the session status
        # and would need to poll if it did since stripe is the source of truth
        api_checkout_session_status = invoke_get_checkout_session_status_api(uuid)
        assert api_checkout_session_status is not None, "Expected a valid checkout session status from Stripe API while WAITING_FOR_USER"
        if api_checkout_session_status is not None and api_checkout_session_status.get("status") == "open":
            logging.debug(f"DEBUG: Current setup checkout session is still open for user {uuid}, returning existing session")
            return api_checkout_session_status

    # We need to create a new checkout session
    assert curr_payment_setup_status in [ecwp.PaymentSetupStatus.NOT_STARTED, ecwp.PaymentSetupStatus.SUCCEEDED] or (api_checkout_session_status is not None and api_checkout_session_status.get("status") != "open"), "Unexpected payment setup status"
    logging.debug(f"DEBUG: Current setup checkout session is not open for user {uuid}, creating a new session")
    api_checkout_session_status = invoke_setup_checkout_session_api(uuid)
    payment_to_insert = ecwp.Payment()
    payment_to_insert.payment_setup_status = ecwp.PaymentSetupStatus.WAITING_FOR_USER
    payment_to_insert.pending_setup_session = api_checkout_session_status
    payment_db.upsert_state(esas.StateName.PAYMENT, payment_to_insert)
    return api_checkout_session_status

def invoke_setup_checkout_session_api(uuid):
    success_url = STRIPE_SUCCESS_URL
    if not success_url:
        raise ValueError("STRIPE_SUCCESS_URL is required for setup checkout")

    cancel_url = STRIPE_CANCEL_URL
    payload = {
        "mode": "setup",
        "success_url": success_url,
        "currency": "USD",
    }
    if cancel_url:
        payload["cancel_url"] = cancel_url

    logging.warning("About to invoke the remote call now=%s" % payload)

    logging.warning("Invoking stripe checkout.Session.create with payload=%s" % payload)
    session = stripe.checkout.Session.create(**payload)
    json_session = json.loads(str(session))

    logging.debug("Received stripe checkout.Session.create response: %s" % json_session)
    return json_session

def invoke_get_checkout_session_status_api(uuid):
    """
    Polling call to check if the setup checkout session has been completed. This is a trigger
    to retrieve the reusable payment method from the checkout session and store it in the database.
    """
    payment_db = esas.StateStorage.get_state_storage(uuid)
    payment_info = payment_db.get_current_state(esas.StateName.PAYMENT)
    logging.debug(f"DEBUG: Retrieved payment info for user {uuid}: {payment_info}")
    if payment_info is None:
        logging.debug(f"DEBUG: No current payment session found for user {uuid}, returning None")
        return None
    elif payment_info.get("pending_setup_session") is None:
        logging.debug(f"DEBUG: No pending setup session found for user {uuid}, returning None")
        return None
    else:
        assert payment_info.get("pending_setup_session") is not None, "Expected a pending setup session for user %s" % uuid
        setup_checkout_session_id = payment_info.get("pending_setup_session", {}).get("id")
        logging.debug(f"DEBUG: Found pending setup session for user {uuid}: {setup_checkout_session_id}, retrieving from stripe")
        expanded_session = stripe.checkout.Session.retrieve(
            setup_checkout_session_id,
            expand=["setup_intent"],
        )
        return json.loads(str(expanded_session))

def setup_checkout_session_resolved(uuid):
    """
    Polling call to check if the setup checkout session has been completed. This is a trigger
    to retrieve the reusable payment method from the checkout session and store it in the database.
    """
    payment_db = esas.StateStorage.get_state_storage(uuid)
    payment_info = payment_db.get_current_state(esas.StateName.PAYMENT)
    if payment_info is None or payment_info.get("pending_setup_session") is None:
        raise ValueError("No pending setup session found for user %s" % uuid)
    if payment_info.payment_setup_status != ecwp.PaymentSetupStatus.WAITING_FOR_USER:
        raise ValueError("Setup checkout session is not in WAITING_FOR_USER state for user %s" % uuid)

    # We cannot use our cached value because it may be stale.
    # We need to call the API to get the current status of the session.
    setup_checkout_session_status = invoke_get_checkout_session_status_api(uuid)
    if setup_checkout_session_status is None or setup_checkout_session_status.get("setup_intent") is None:
        raise ValueError(f"For {uuid=}, checkout session is not valid, {setup_checkout_session_status=} and {setup_checkout_session_status.get('setup_intent')=}")
    
    if setup_checkout_session_status.get("setup_intent", {}).get("status") != "succeeded":
        raise ValueError(f"For {uuid=}, setup checkout session has not succeeded yet; current status: {setup_checkout_session_status.get('setup_intent').get('status')}")

    if setup_checkout_session_status.get("setup_intent", {}).get("payment_method") is None:
        raise ValueError(f"For {uuid=}, setup checkout session has succeeded but no payment method was set; this should not happen")

    # At this point, we should have a valid setup checkout session and can use it freely
    payment_info.payment_setup = setup_checkout_session_status.get("setup_intent")
    payment_info.payment_setup_status = ecwp.PaymentSetupStatus.SUCCEEDED
    del payment_info.pending_setup_session
    logging.debug(f"About to save the updated payment info for {uuid=}: {payment_info}")

    esas.StateStorage.get_state_storage(uuid).upsert_state(esas.StateName.PAYMENT, payment_info)
    logging.debug(f"DEBUG: Returning from setup_checkout_session_resolved for user {uuid}")
    return setup_checkout_session_status