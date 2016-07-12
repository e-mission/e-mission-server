import logging
import emission.core.wrapper.wrapperbase as ecwb
import emission.core.wrapper.motionactivity as ecwm

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
    props = dict([(t.name, ecwb.WrapperBase.Access.WORM) for t in ecwm.MotionTypes])
    props.update(
        {'ts': ecwb.WrapperBase.Access.WORM,  # YYYY-MM-DD
         'local_dt': ecwb.WrapperBase.Access.WORM,
         'fmt_time': ecwb.WrapperBase.Access.WORM}    # YYYY-MM-DD
    )

    enums = {}
    geojson = []
    nullable = []
    local_dates = ['local_dt']

    def _populateDependencies(self):
        pass
