from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb

class TourModel(ecwb.WrapperBase):

    props = {"user_id" : ecwb.WrapperBase.Access.WORM, # user_id of the E-Missions user the graph represnts 
    }

    geojson = []
    enums = {}
    nullable = []
    local_dates = []

    def _populateDependencies(self):
        pass
