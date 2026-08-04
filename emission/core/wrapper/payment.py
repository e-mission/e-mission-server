"""
Vehicle data model for deployments that utilize a library or fleet of shared vehicles.
Tracks vehicle location (dock/bay/parking spot ID or user UUID if checked out) and active reservations.
"""
from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb

class Payment(ecwb.WrapperBase):
    props = {
        "placeholder": ecwb.WrapperBase.Access.RW,
    }

    def _populateDependencies(self):
        pass
