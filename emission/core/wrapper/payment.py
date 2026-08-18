"""
Vehicle data model for deployments that utilize a library or fleet of shared vehicles.
Tracks vehicle location (dock/bay/parking spot ID or user UUID if checked out) and active reservations.
"""
from builtins import *
import enum
import logging
import emission.core.wrapper.wrapperbase as ecwb

class PaymentSetupStatus(enum.Enum):
    NOT_STARTED = 0
    WAITING_FOR_USER = 1
    EXPIRED = 2
    SUCCEEDED = 3
    FAILED = 4

class Payment(ecwb.WrapperBase):
    props = {
        "payment_setup_status": ecwb.WrapperBase.Access.RW,  # the status of the payment setup process
        "pending_setup_session": ecwb.WrapperBase.Access.RW, # while waiting for setup
        "payment_setup": ecwb.WrapperBase.Access.RW, # the setup intent returned by stripe after successful setup
    }

    enums = {
        "payment_setup_status": PaymentSetupStatus
    }
    geojson = []
    nullable = []
    local_dates = []

    def _populateDependencies(self):
        pass
