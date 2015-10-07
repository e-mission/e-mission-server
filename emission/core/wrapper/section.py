import logging
import emission.core.wrapper.wrapperbase as ecwb
import emission.core.wrapper.motionactivity as ecwm

# TODO: Figure out whether we should parse the geojson back to a geojson object

class Section(ecwb.WrapperBase):
    props = {"user_id": ecwb.WrapperBase.Access.WORM, # start UTC timestamp (in secs)
             "trip_id": ecwb.WrapperBase.Access.WORM, # the trip that this is part of

             "start_ts": ecwb.WrapperBase.Access.WORM, # start UTC timestamp (in secs)
             "start_local_dt": ecwb.WrapperBase.Access.WORM, # searchable datatime in local time of start location
             "start_fmt_time": ecwb.WrapperBase.Access.WORM, # start formatted time (in timezone of point)
             "end_ts": ecwb.WrapperBase.Access.WORM, # end UTC timestamp (in secs)
             "end_local_dt": ecwb.WrapperBase.Access.WORM, # searchable datetime in local time of end location
             "end_fmt_time": ecwb.WrapperBase.Access.WORM, # end formatted time (in timezone of point)
             "start_stop": ecwb.WrapperBase.Access.WORM,  # _id of place object before this one
             "end_stop": ecwb.WrapperBase.Access.WORM,    # _id of place object after this one
             "start_loc": ecwb.WrapperBase.Access.WORM,    # location of start point in geojson format
             "end_loc": ecwb.WrapperBase.Access.WORM,      # location of end point in geojson format
             "duration": ecwb.WrapperBase.Access.WORM,     # duration of the trip in secs
             "distance": ecwb.WrapperBase.Access.WORM,     # distance of the trip in meters
             "sensed_mode": ecwb.WrapperBase.Access.WORM,  # the sensed mode used for the segmentation
             "source": ecwb.WrapperBase.Access.WORM}       # the method used to generate this trip

    enums = {"sensed_mode": ecwm.MotionTypes}
    geojson = ["start_loc", "end_loc"]
    nullable = ["start_stop", "end_stop"]

    def _populateDependencies(self):
        pass
