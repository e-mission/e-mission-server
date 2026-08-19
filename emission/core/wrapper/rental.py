"""
Vehicle data model for deployments that utilize a library or fleet of shared vehicles.
Tracks vehicle location (dock/bay/parking spot ID or user UUID if checked out) and active reservations.
"""
from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb

class Rental(ecwb.WrapperBase):
    props = {
        "vehicle_id": ecwb.WrapperBase.Access.RW,  # unique identifier
        "vehicle_name": ecwb.WrapperBase.Access.RW,  # display name/description
        "payment_hold_info": ecwb.WrapperBase.Access.RW,  # Stripe PaymentIntent id for the active hold
        "rental_start_ts": ecwb.WrapperBase.Access.RW, # time
        "rental_end_ts": ecwb.WrapperBase.Access.RW,  # e.g., CAR, BIKE, E_BIKE
        "rental_status": ecwb.WrapperBase.Access.RW,
    }

    enums = {}
    geojson = []
    nullable = []
    local_dates = []

    def _populateDependencies(self):
        pass

