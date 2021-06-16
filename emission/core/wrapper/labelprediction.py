# Based on modeprediction.py
import emission.core.wrapper.wrapperbase as ecwb

class Labelprediction(ecwb.WrapperBase):
    props = {"trip_id":   ecwb.WrapperBase.Access.WORM,  # the trip that this is part of
            "prediction": ecwb.WrapperBase.Access.WORM,  # What we predict
            "start_ts":   ecwb.WrapperBase.Access.WORM,  # start time for the prediction, so that it can be captured in time-based queries, e.g. to reset the pipeline
            "end_ts":     ecwb.WrapperBase.Access.WORM,  # end time for the prediction, so that it can be captured in time-based queries, e.g. to reset the pipeline
    }

    enums = {}
    geojson = {}
    local_dates = {}

    def _populateDependencies(self):
        pass
