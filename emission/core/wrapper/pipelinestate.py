import logging
import emission.core.wrapper.wrapperbase as ecwb
import enum as enum

class PipelineStages(enum.Enum):
    USERCACHE = 0
    ACCURACY_FILTERING = 6
    TRIP_SEGMENTATION = 1
    SECTION_SEGMENTATION = 2
    JUMP_SMOOTHING = 3
    MODE_INFERENCE = 4
    TOUR_MODEL = 5
    ALTERNATIVES = 6
    USER_MODEL = 7
    RECOMMENDATION = 8
    OUTPUT_GEN = 9

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

    def _populateDependencies(self):
        pass
