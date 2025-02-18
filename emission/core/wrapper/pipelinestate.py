from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb
import enum as enum

class PipelineStages(enum.Enum):
    USERCACHE = 0
    USER_INPUT_MATCH_INCOMING = 12
    ACCURACY_FILTERING = 6
    TRIP_SEGMENTATION = 1
    SECTION_SEGMENTATION = 2
    JUMP_SMOOTHING = 3
    CLEAN_RESAMPLING = 11
    MODE_INFERENCE = 4
    LABEL_INFERENCE = 14
    TRIP_MODEL = 16
    EXPECTATION_POPULATION = 15
    CREATE_CONFIRMED_OBJECTS = 13
    CREATE_COMPOSITE_OBJECTS = 17
    TOUR_MODEL = 5
    ALTERNATIVES = 10
    USER_MODEL = 7
    RECOMMENDATION = 8
    OUTPUT_GEN = 9
    EXPORT_DATA = 18
    PURGE_TIMESERIES_DATA = 19
    RESTORE_TIMESERIES_DATA = 20

class PipelineState(ecwb.WrapperBase):
    props = {"pipeline_stage": ecwb.WrapperBase.Access.RW,  # the value of the stage from the enum above
             "user_id": ecwb.WrapperBase.Access.RW, # the user whose state this is
             "curr_run_ts": ecwb.WrapperBase.Access.RW, # the last time in the currrent run (only set if the stage is currently running)
             "last_processed_ts": ecwb.WrapperBase.Access.RW, # the last (client-generated) timestamp upto which we have finished processing the data
             "last_ts_run": ecwb.WrapperBase.Access.RW     # the last time that this stage was run
            }

    enums = {"pipeline_stage": PipelineStages}
    geojson = []
    nullable = []
    local_dates = []

    def _populateDependencies(self):
        pass
