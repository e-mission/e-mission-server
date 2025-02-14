import logging
import pandas as pd
import copy

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.pipeline_queries as epq

import emission.analysis.config as eac
import emission.analysis.intake.domain_assumptions as eaid
import emission.analysis.section_features as easf

import emission.net.ext_service.transit_matching.match_stops as enetm

import emission.core.wrapper.modeprediction as ecwm
import emission.core.wrapper.motionactivity as ecwma
import emission.core.wrapper.entry as ecwe

def predict_mode(user_id):
    time_query = epq.get_time_range_for_mode_inference(user_id)
    try:
        mip = RuleEngineModeInferencePipeline()
        mip.user_id = user_id
        mip.runPredictionPipeline(user_id, time_query)
        if mip.getLastSectionDone() is None:
            logging.debug("after, run, last_section_done == None, must be early return")
            epq.mark_mode_inference_done(user_id, None)
            return
        else:
            epq.mark_mode_inference_done(user_id, mip.getLastSectionDone())
    except:
        logging.exception("Error while inferring modes, timestamp is unchanged")
        epq.mark_mode_inference_failed(user_id)

class RuleEngineModeInferencePipeline:
    def __init__(self):
        self.last_section_done = None

    def getLastSectionDone(self):
        return self.last_section_done
  
    def runPredictionPipeline(self, user_id, timerange):
        self.user_id = user_id
        self.ts = esta.TimeSeries.get_time_series(user_id)
        self.toPredictSections = esda.get_entries(esda.CLEANED_SECTION_KEY, user_id, 
            time_query=timerange)
        if (len(self.toPredictSections) == 0):
            logging.debug("len(toPredictSections) == 0, early return")
            if self.last_section_done is not None:
                logging.error("self.last_section_done == %s, expecting None" %
                    self.last_section_done)
                if eac.get_config()["classification.validityAssertions"]:
                    assert False
            return None
    
        self.predictedProb = self.predictModesStep()
        logging.info("predictModesStep DONE")
        self.savePredictionsStep()
        logging.info("savePredictionsStep DONE")

    def predictModesStep(self):
        """
        Converts list of sections -> list of predicted probability maps
        """
        predictedProb = []
        locs_where_no_prediction = []

        # first pass; air, walk, and bike will be identified
        # without needing to query overpass
        for (i, section_entry) in enumerate(self.toPredictSections):
            logging.debug('~' * 10 + 
                "About to get predicted value for section %s (%s -> %s)" %
                (section_entry.get_id(),
                 section_entry.data.start_fmt_time, section_entry.data.end_fmt_time) +
                '~' * 10)
            if section_entry.data.sensed_mode == ecwma.MotionTypes.AIR_OR_HSR:
                predictedProb.append({'AIR_OR_HSR': 1})
            else:
                prediction = get_prediction(section_entry)
                if prediction is None:
                    locs_where_no_prediction.append(section_entry.data.start_loc['coordinates'])
                    locs_where_no_prediction.append(section_entry.data.end_loc['coordinates'])
                predictedProb.append(prediction)

        # for sections that we could not predict on first pass,
        # query overpass for public transit stops near the start/end locations
        stop_radius = eac.get_config()["section.startStopRadius"]
        transit_stops_map = dict(zip(
            [str(loc) for loc in locs_where_no_prediction],
            enetm.get_stops_near(locs_where_no_prediction, stop_radius),
        ))

        # second pass; now that we have transit stops, try to predict sections
        # that we couldn't predict before
        for (i, section_entry) in enumerate(self.toPredictSections):
            if predictedProb[i] is None:
                predictedProb[i] = get_prediction(section_entry, transit_stops_map)

        return predictedProb

    def savePredictionsStep(self):
        for i, currProb in enumerate(self.predictedProb):
            currSectionEntry = self.toPredictSections[i]
            currSection = currSectionEntry.data
            currProb = currProb
    
            # Insert the prediction
            mp = ecwm.Modeprediction()
            mp.trip_id = currSection.trip_id
            mp.section_id = currSectionEntry.get_id()
            mp.algorithm_id = ecwm.AlgorithmTypes.SIMPLE_RULE_ENGINE
            mp.predicted_mode_map = currProb
            mp.start_ts = currSection.start_ts
            mp.end_ts = currSection.end_ts
            self.ts.insert_data(self.user_id, "inference/prediction", mp)
    
            # There are now two predictions, but don't want to do a bunch of
            # refactoring, so just create the inferred # section object right here
            is_dict = copy.copy(currSectionEntry)
            del is_dict["_id"]
            is_dict["metadata"]["key"] = "analysis/inferred_section"
            is_dict["data"]["sensed_mode"] = ecwm.PredictedModeTypes[easf.select_inferred_mode([mp])].value
            is_dict["data"]["cleaned_section"] = currSectionEntry.get_id()
            ise = ecwe.Entry(is_dict)
            logging.debug("Updating sensed mode for section = %s to %s" % 
                (currSectionEntry.get_id(), ise.data.sensed_mode))
            self.ts.insert(ise)
        # Set last_section_done after saving because otherwise if there is an error
        # during inference, we will not save results and never re-run
        self.last_section_done = self.toPredictSections[-1]

def get_prediction(section_entry, transit_stops_map=None):
    if section_entry.data.sensed_mode == ecwma.MotionTypes.UNKNOWN:
        return get_unknown_prediction(section_entry, transit_stops_map)
    elif eaid.is_motorized(section_entry.data.sensed_mode):
        return get_motorized_prediction(section_entry, transit_stops_map)
    else:
        return get_non_motorized_prediction(section_entry)

def get_non_motorized_prediction(section_entry):
    """
    This one is relatively easy. Only non-motorized predictions are WALK and BIKE.
    If 75% speed < walking speed, WALK
    else BIKE

    We can also generate a confidence for the prediction depending on the
    magnitude of the difference from the walking speed, but let's start with
    something simple here.
    """

    if eaid.is_walking_speed(pd.Series(section_entry.data["speeds"]).median()):
        return {'WALKING': 1}
    else:
        return {'BICYCLING': 1} 

def get_motorized_prediction(section_entry, transit_stops_map=None):
    """
    This is a bit trickier, but still relatively simple.
    First, we get the stops for the beginning and end of the section.
    If there is overlap between the stops, we name it transit, otherwise we
    name it car.
    """
    if transit_stops_map is None:
        return None
    predicted_transit_mode = _get_transit_prediction(section_entry, transit_stops_map)
    if predicted_transit_mode is not None:
        return {predicted_transit_mode: 1}
    else:
        return {'CAR': 1}

def get_unknown_prediction(section_entry, transit_stops_map=None):
    """
    For unknown sections, we sometimes have points and sometimes not. If we
    have points, use the median speed as always. Otherwise, we cannot to fine
    grained speed specific checks. We can do checks on overall speed and GIS
    features. If nothing matches, we should probably leave as UNKNOWN.
    """
    if len(section_entry.data["speeds"]) > 0 and \
        eaid.is_walking_speed(pd.Series(section_entry.data["speeds"]).median()):
        logging.debug("median speed is walking, UNKNOWN -> WALKING")
        return {'WALKING': 1}
    if eaid.is_walking_speed(easf.calOverallSectionSpeed(section_entry.data)):
        logging.debug("overall speed is walking, UNKNOWN -> WALKING")
        return {'WALKING': 1}
    else:
        if transit_stops_map is None:
            return None
        predicted_transit_mode = _get_transit_prediction(section_entry, transit_stops_map)
        if predicted_transit_mode is not None:
            logging.debug("predicted transit mode is not None, UNKNOWN -> %s" % 
                predicted_transit_mode)
            return {predicted_transit_mode: 1}
        else:
            # could be either car or bike, dunno which
            logging.debug("no luck finding a match, stay UNKNOWN")
            return {'UNKNOWN': 1}

def _get_transit_prediction(section_entry, transit_stops_map=None):
    if transit_stops_map is None:
        return None
    start_transit_stops = transit_stops_map.get(str(section_entry.data.start_loc['coordinates']))
    end_transit_stops = transit_stops_map.get(str(section_entry.data.end_loc['coordinates']))
    print(f'start_transit_stops = {start_transit_stops}, end_transit_stops = {end_transit_stops}')
    predicted_transit_modes = enetm.get_predicted_transit_mode(start_transit_stops,
        end_transit_stops)
    logging.debug("Got predicted transit mode %s" % predicted_transit_modes)
    collapsed_transit_mode = collapse_modes(section_entry, predicted_transit_modes)
    return collapsed_transit_mode

def collapse_modes(section_entry, modes):
    """
    GIS supports a rich set of transit modes (e.g. train, subway, light_rail),
    but we only support the basic train. We can add support for the others, but
    not without changing the client and we don't want to make those changes now.
    
    Also, there could be multiple parallel modes (e.g. bus and train) at both
    start and end points. This method merges the list of entries returned by
    the GIS into one, potentially using speed information 
    """
    # train_mode_list = ['funicular', 'miniature', 'rail', 'railway',
    #     'light_rail', 'subway', 'monorail', 'tram', 'aerialway', ]
    train_mode_list = ['funicular', 'miniature', 'rail', 'railway', 'monorail', 
                'monorail', 'aerialway', 'tracks']

    if modes is None or len(modes) == 0:
        return None

    modes_no_suspend = [m.replace(":suspended", "").replace(":SUSPENDED", "")
                            for m in modes]

    # map all train-like modes to train
    map_train = lambda m: 'TRAIN' if m in train_mode_list else m.upper()
    train_mapped_modes = list(map(map_train, modes_no_suspend))

    logging.debug("train_mapped_modes = %s" % train_mapped_modes)

    if len(train_mapped_modes) == 1:
        return train_mapped_modes[0]

    # now uniquify them. If there is only one possible mode (e.g. train), there
    # should be only one entry here
    unique_modes = set(train_mapped_modes)

    logging.debug("unique_modes = %s" % unique_modes)

    if len(unique_modes) == 1:
        return list(unique_modes)[0]

    supported_modes = set(['BUS', 'TRAIN', 'LIGHT_RAIL', 'SUBWAY', 'TRAM'])

    if not unique_modes.issubset(supported_modes):
        logging.error("unique_modes = %s, but we support only %s" %
            (unique_modes, supported_modes))
        if eac.get_config()["classification.validityAssertions"]:
            assert False
   
    # could be either bus or train. Let's use the speed to decide
    # local bus speeds are pretty close to bike, which is why it is hard to
    # distinguish between them
    if eaid.is_bicycling_speed(pd.Series(section_entry.data["speeds"]).median()):
        return 'BUS'
    else:
        # We only want to return a train modes there, so let's remove bus from the list
        if 'BUS' in train_mapped_modes:
            train_mapped_modes.remove("BUS")
        logging.debug("Returning %s but could also be another train modes" % train_mapped_modes[0])
        return train_mapped_modes[0]
