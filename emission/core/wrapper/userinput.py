from builtins import *
import logging
import enum
import emission.core.wrapper.wrapperbase as ecwb

class InputStatus(str, enum.Enum):
    ACTIVE = "ACTIVE"
    DELETED = "DELETED"

class Userinput(ecwb.WrapperBase):
    props = {"status": ecwb.WrapperBase.Access.RO, # ACTIVE or DELETED
             "label": ecwb.WrapperBase.Access.RO, # string summary of the survey
             "version": ecwb.WrapperBase.Access.RO, # the survey version
             "name": ecwb.WrapperBase.Access.RO, # the survey name
             "xmlResponse": ecwb.WrapperBase.Access.RO, # the XML string representation of the survey response
             "jsonDocResponse": ecwb.WrapperBase.Access.RO # the JSON representation of the survey response
    }

    enums = {"status": InputStatus}
    geojson = []
    nullable = []
    local_dates = ["start_local_dt", "end_local_dt"]

    def _populateDependencies(self):
        pass
