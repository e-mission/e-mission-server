"""
Vehicle data model for deployments that utilize a library or fleet of shared vehicles.
Tracks vehicle location (dock/bay/parking spot ID or user UUID if checked out) and active reservations.
"""
from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb

class Payment(ecwb.WrapperBase):
    props = {
        # TODO: Should this be WORM or RW? we should not really need to reset
        # the customer after the initial sign up, but I am not sure if I am
        # missing some use case.
        "customer_id": ecwb.WrapperBase.Access.WORM,  # set once during creation
        # This needs to be RW to allow users to reconfigure the payment method
        # as credit cards expire etc
        "payment_method_id": ecwb.WrapperBase.Access.RW,  # can be reconfigured as
        # This needs to be RW because it will be set when we are waiting for the
        # user to respond, but will be removed after that
        "pending_setup_session_id": ecwb.WrapperBase.Access.RW, # while waiting for setup
        "pending_setup_session_url": ecwb.WrapperBase.Access.RW,
    }

    enums = {}
    geojson = []
    nullable = []
    local_dates = []

    def _populateDependencies(self):
        pass
