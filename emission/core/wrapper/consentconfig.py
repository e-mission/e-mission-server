from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
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
