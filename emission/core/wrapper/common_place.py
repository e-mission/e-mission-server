from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb

class CommonPlace(ecwb.WrapperBase):
    props = {
         "location" : ecwb.WrapperBase.Access.WORM, # location in geojson format
         # TODO: Currently marking this as RW to get around the fact that I need
         # to set the successors after creation.
         # Need to revisit after I fix the model creation and multiple place generation
         "successors" : ecwb.WrapperBase.Access.RW, # set of CommonPlaces connected to this location
         "places": ecwb.WrapperBase.Access.WORM, # Set of place_ids that map to this common place
         "user_id" : ecwb.WrapperBase.Access.WORM,
    }

    geojson = ["location"]
    enums = {}
    nullable = []
    local_dates = []

    def _populateDependencies(self):
        pass
