import os
import logging
from webbrowser import get
import stripe
import emission.storage.modifiable.abstract_state_storage as esas
import emission.core.wrapper.payment as ecwp

# BEGIN DO NOT REFACTOR: I do not want to wrap these accesses
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY")
STRIPE_SUCCESS_URL = os.environ.get("STRIPE_SUCCESS_URL")
STRIPE_CANCEL_URL = os.environ.get("STRIPE_CANCEL_URL")
STRIPE_API_BASE = "https://api.stripe.com"
# END DO NOT REFACTOR: I do not want to wrap these accesses

logging.basicConfig(level=logging.DEBUG)

logging.debug("About to configure stripe with STRIPE_SECRET_KEY=%s,\
               STRIPE_API_BASE=%s" % (
    STRIPE_SECRET_KEY[:5] + "..." if STRIPE_SECRET_KEY else None,
    STRIPE_API_BASE,
))
stripe.api_key = STRIPE_SECRET_KEY
stripe.api_base = STRIPE_API_BASE

# BEGIN: Setup flow; checkout session with a "setup" mode, and a hosted URL.
# This requires a call to set up the session, which returns the hosted URL to the phone
# a call that is initiated by the phone once the user has completed the setup on the stripe site,
# and a call to poll for the current session status, so that the phone can handle edge conditions
# related to cancellation and not opening the app success URL

def create_setup_checkout_session(uuid):
    logging.debug("Creating stripe setup checkout session for user %s" % uuid)
    payment_db = esas.StateStorage.get_state_storage(uuid)
    curr_payment_session = payment_db.get_current_state(esas.StateName.PAYMENT)
    if curr_payment_session is not None:
        session_status = get_setup_checkout_session_status(uuid)
        logging.warning(f"WARNING: Retrieved current setup checkout session status for user {uuid}: {session_status=}")
        if session_status is not None and session_status.status == "open":
            ret_val = {"id": curr_payment_session.get("pending_setup_session_id"),
                       "url": curr_payment_session.get("pending_setup_session_url")}
            logging.debug(f"DEBUG: Early return from create_setup_checkout_session with {ret_val=},"
                          f"skipping call to stripe backend ")
            return ret_val
        else:
            logging.debug(f"DEBUG: Current setup session {curr_payment_session.get('pending_setup_session_id')} "
                          f"has status {session_status.status if session_status is not None else None}, creating a new session")

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

    logging.warning("Received stripe checkout.Session.create response: %s" % session)

    # TODO: Determine which of these fields we want to store for future use
    payment_db.upsert_state(esas.StateName.PAYMENT, ecwp.Payment({
        "pending_setup_session_id": session.id,
        "pending_setup_session_url": session.url,
    }))

    if not session.id or not session.url:
        raise ValueError("Invalid Stripe setup checkout response: missing id or url")
    ret_value = {
        "id": session.id,
        "url": session.url,}
    logging.debug(f"DEBUG: Returning from create_setup_checkout_session with {ret_value=}")
    logging.warning(f"WARN: Returning from create_setup_checkout_session with {ret_value=}")
    return ret_value

def get_setup_checkout_session_status(uuid):
    """
    Polling call to check if the setup checkout session has been completed. This is a trigger
    to retrieve the reusable payment method from the checkout session and store it in the database.
    """
    current_payment_session = esas.StateStorage.get_state_storage(uuid).get_current_state(esas.StateName.PAYMENT)
    logging.warning(f"WARN: Retrieved current payment session for user {uuid}: {current_payment_session=}")
    if current_payment_session is not None and current_payment_session.get("pending_setup_session_id") is not None:
        setup_checkout_session_id = current_payment_session.get("pending_setup_session_id")
        expanded_session = stripe.checkout.Session.retrieve(
            setup_checkout_session_id,
            expand=["setup_intent"],
        )
        logging.debug(f"Retrieved setup checkout session {setup_checkout_session_id} with {expanded_session=}")
        setup_intent_object = expanded_session.setup_intent
        logging.debug(f"setup_intent_object={setup_intent_object}")

        if setup_intent_object is None:
            raise ValueError("Checkout Session has no setup_intent; run setup checkout again")
        return expanded_session
    else:
        # TODO: Determine which of these fields are relevant to return, both for the
        # server and for the phone app
        return None

def setup_checkout_session_resolved(uuid):
    """
    Polling call to check if the setup checkout session has been completed. This is a trigger
    to retrieve the reusable payment method from the checkout session and store it in the database.
    """
    setup_checkout_session_status = get_setup_checkout_session_status(uuid)
    if setup_checkout_session_status.setup_intent.status != "succeeded":
        raise ValueError("Setup checkout session has not succeeded yet; run setup checkout again")
    esas.StateStorage.get_state_storage(uuid).upsert_state(esas.StateName.PAYMENT,
        ecwp.Payment({"setup_checkout_session_id": setup_checkout_session_status.id,
                      "customer_id": setup_checkout_session_status.setup_intent.customer,
                      "payment_method_id": setup_checkout_session_status.setup_intent.payment_method}))
    ret_val = {
        "setup_checkout_session_id": setup_checkout_session_status.id,
        "customer_id": setup_checkout_session_status.setup_intent.customer,
        "payment_method_id": setup_checkout_session_status.setup_intent.payment_method
    }
    logging.debug(f"DEBUG: Returning from setup_checkout_session_resolved with {ret_val=}")
    return ret_val