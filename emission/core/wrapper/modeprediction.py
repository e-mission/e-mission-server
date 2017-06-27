import logging
import emission.core.wrapper.wrapperbase as ecwb
import emission.core.wrapper.motionactivity as ecwm
import enum as enum


class AlgorithmTypes(enum.Enum):
	'''
	We could use NOT_PREDICTED to indicate a prediction that is empty. If a prediction returns zero,
		it can be incorrectly assumed that the mode predicted it IN_VEHICLE if the prediction returned an error or was never
		made in the first place. Using NOT_PREDICTED can help distinguish the true zeros from the erroneous ones.

	'''
	NOT_PREDICTED = 0
	RANDOM_FOREST = 1


class Modeprediction(ecwb.WrapperBase):
	props = {"trip_id": 	ecwb.WrapperBase.Access.WORM, 	# *******the trip that this is part of
            "section_id": 	ecwb.WrapperBase.Access.WORM, 	#The section id that this prediction corresponds to
            "algorithm_id": ecwb.WrapperBase.Access.WORM, 	#The algorithm which made this prediction
            "confirmed_mode": ecwb.WrapperBase.Access.WORM, #The mode inputted by the user (if it exists)
            "sensed_mode": 	ecwb.WrapperBase.Access.WORM, 	#THe mode that the phones sensors picked up
            "predicted_mode": ecwb.WrapperBase.Access.WORM, #What we predicted
            "confidence":	ecwb.WrapperBase.Access.WORM, 	#Our confidence in the prediction 


             # "start_ts": ecwb.WrapperBase.Access.WORM, # ****start UTC timestamp (in secs)
             # "start_local_dt": ecwb.WrapperBase.Access.WORM, # ********searchable datatime in local time of start location
             # "start_fmt_time": ecwb.WrapperBase.Access.WORM, # ******start formatted time (in timezone of point)
             # "end_ts": ecwb.WrapperBase.Access.WORM, # ******end UTC timestamp (in secs)
             # "end_local_dt": ecwb.WrapperBase.Access.WORM, #***** searchable datetime in local time of end location
             # "end_fmt_time": ecwb.WrapperBase.Access.WORM, # ******end formatted time (in timezone of point)
             # "start_stop": ecwb.WrapperBase.Access.WORM,  # _id of place object before this one
             # "end_stop": ecwb.WrapperBase.Access.WORM,    # _id of place object after this one
             # "start_loc": ecwb.WrapperBase.Access.WORM,    # location of start point in geojson format
             # "end_loc": ecwb.WrapperBase.Access.WORM,      # location of end point in geojson format
             # "duration": ecwb.WrapperBase.Access.WORM,     # duration of the trip in secs
             # "sensed_mode": ecwb.WrapperBase.Access.WORM,  # ****the sensed mode used for the segmentation
             # "source": ecwb.WrapperBase.Access.WORM}        # *****the method used to generate this trip
             }
    enums = {"sensed_mode": ecwm.MotionTypes,
    		"predicted_mode": ecwm.MotionTypes,
    		"confirmed_mode": ecwm.MotionTypes,
    		"algorithm_id": AlgorithmTypes }
    geojson = ["start_loc", "end_loc"]
    nullable = ["start_stop", "end_stop", "confirmed_mode", "predicted_mode"]
    local_dates = ['start_local_dt', 'end_local_dt']


def _populateDependencies(self):
    pass
