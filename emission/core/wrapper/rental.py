"""
Vehicle data model for deployments that utilize a library or fleet of shared vehicles.
Tracks vehicle location (dock/bay/parking spot ID or user UUID if checked out) and active reservations.
"""
from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb

class Rental(ecwb.WrapperBase):
    props = {
        "vehicle_id": ecwb.WrapperBase.Access.WORM,  # unique identifier
        "vehicle_name": ecwb.WrapperBase.Access.WORM,  # display name/description
        "payment_hold_info": ecwb.WrapperBase.Access.WORM,  # Stripe PaymentIntent id for the active hold
        "start_ts": ecwb.WrapperBase.Access.WORM,
        "start_local_dt": ecwb.WrapperBase.Access.WORM,
        "start_fmt_time": ecwb.WrapperBase.Access.WORM,
        "end_ts": ecwb.WrapperBase.Access.WORM,
        "end_local_dt": ecwb.WrapperBase.Access.WORM,
        "end_fmt_time": ecwb.WrapperBase.Access.WORM,
        "start_dock_id": ecwb.WrapperBase.Access.WORM,
        "end_dock_id": ecwb.WrapperBase.Access.WORM,
        "rental_status": ecwb.WrapperBase.Access.RW,
    }

    enums = {}
    geojson = []
    nullable = []
    local_dates = ['start_local_dt', 'end_local_dt']

    def _populateDependencies(self):
        pass

