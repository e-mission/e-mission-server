from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb
import emission.core.wrapper.motionactivity as ecwm
import emission.core.wrapper.modeprediction as ecwmp

# Used for various metrics such as count, distance, mean speed calorie consumption,
# median speed calorie consumption
# Should come later: carbon footprint
# Optimal doesn't look like it fits this, because it is not per mode

class ModeStatTimeSummary(ecwb.WrapperBase):
    # We will end up with props like
    # {
    #    MotionTypes.IN_VEHICLE: ecwb.WrapperBase.Access.WORM
    # }
    # Each distance will have
    #
    #
    # Make this only predicted mode, or remove completely depending on what we
    # do for mode stuff
    props = dict([(t.name, ecwb.WrapperBase.Access.WORM) for t in ecwm.MotionTypes])
    props.update(dict([(t.name, ecwb.WrapperBase.Access.WORM) for t in ecwmp.PredictedModeTypes]))
    props.update(
        {'ts': ecwb.WrapperBase.Access.WORM,  # YYYY-MM-DD
         'local_dt': ecwb.WrapperBase.Access.WORM,
         'fmt_time': ecwb.WrapperBase.Access.WORM,
         'nUsers': ecwb.WrapperBase.Access.WORM} # Relevant in the
        # aggregate case, when we want to see how many users we have
        # aggregated data from so that we can compute avg, etc
    )

    enums = {}
    geojson = []
    nullable = []
    local_dates = ['local_dt']

    def _populateDependencies(self):
        pass
