"""
Vehicle data model for deployments that utilize a library or fleet of shared vehicles.
Tracks vehicle location (dock/bay/parking spot ID or user UUID if checked out) and active reservations.
"""
from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb

class Vehicle(ecwb.WrapperBase):
    props = {
        "vehicle_id": ecwb.WrapperBase.Access.WORM,  # unique identifier
        "vehicle_name": ecwb.WrapperBase.Access.RW,  # display name/description
        "location": ecwb.WrapperBase.Access.RW,  # dock/bay/parking spot ID, UNINITIALIZED if not commissioned, or None if checked out
        "bluetooth_major_minor": ecwb.WrapperBase.Access.RW,  # list of Bluetooth identifiers, if applicable
        "baseMode": ecwb.WrapperBase.Access.RW,  # e.g., CAR, BIKE, E_BIKE
        "met_equivalent": ecwb.WrapperBase.Access.RW,
        "kgCo2PerKm": ecwb.WrapperBase.Access.RW,
        "vehicle_info": ecwb.WrapperBase.Access.RW,  # {type, license, make, model, year, color, engine, mpge, ...}
        "created_at": ecwb.WrapperBase.Access.WORM,
        "updated_at": ecwb.WrapperBase.Access.RW,
    }

    enums = {}
    geojson = []
    nullable = []
    local_dates = []

    def _populateDependencies(self):
        pass
