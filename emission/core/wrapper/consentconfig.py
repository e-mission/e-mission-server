import logging
import emission.core.wrapper.wrapperbase as ecwb

class Consentconfig(ecwb.WrapperBase):
    props = {"category": ecwb.WrapperBase.Access.RO,  # the category of data collection for which the user has consented
             "protocol_id": ecwb.WrapperBase.Access.RO,
             "approval_date": ecwb.WrapperBase.Access.RO} # the data on which the protocol was approved by the IRB
    enums = {}
    geojson = []
    nullable = []
    local_dates = []

    def _populateDependencies(self):
        pass
