# Standard imports
from pymongo import MongoClient
import logging
import sys
import os
import numpy as np
import scipy as sp
import time
import json
import copy

# Get the configuration for the classifier
import emission.analysis.config as eac

# Our imports
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.decorations.trip_queries as esdt
import emission.storage.pipeline_queries as epq
import emission.analysis.section_features as easf

import emission.core.get_database as edb
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.modeprediction as ecwm
import emission.core.wrapper.motionactivity as ecwma
import emission.core.wrapper.pipelinestate as ecwp

from uuid import UUID

# We are not going to use the feature matrix for analysis unless we have at
# least 50 points in the training set. 50 is arbitrary. We could also consider
# combining the old and new training data, but this is really a bootstrapping
# problem, so we don't need to solve it right now.
minTrainingSetSize = 1000

def predict_mode(user_id):
    time_query = epq.get_time_range_for_mode_inference(user_id)
    try:
        mip = ModeInferencePipeline()
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

# Delete the objects created by this pipeline step (across users)
def del_all_objects(is_dry_run):
    del_query = {}
    del_query.update({"metadata.key": {"$in": ["inference/prediction", "analysis/inferred_section"]}})
    logging.info("About to delete %d entries" 
        % edb.get_analysis_timeseries_db().find(del_query).count())
    logging.info("About to delete entries with keys %s" 
        % edb.get_analysis_timeseries_db().find(del_query).distinct("metadata.key"))

    del_pipeline_query = {"pipeline_stage": ecwp.PipelineStages.MODE_INFERENCE.value}
    logging.info("About to delete pipeline entries for stage %s" %
        ecwp.PipelineStages.MODE_INFERENCE)

    if is_dry_run:
        logging.info("this is a dry-run, returning from del_objects_after without modifying anything")
    else:
        result = edb.get_analysis_timeseries_db().delete_many(del_query)
        logging.info("this is not a dry-run, result of deleting analysis entries is %s" % result.raw_result)
        result = edb.get_pipeline_state_db().delete_many(del_pipeline_query)
        logging.info("this is not a dry-run, result of deleting pipeline state is %s" % result.raw_result)

# Delete the objects created by this pipeline step (for a particular user)
def del_objects_after(user_id, reset_ts, is_dry_run):
    del_query = {}
    # handle the user
    del_query.update({"user_id": user_id})

    del_query.update({"metadata.key": {"$in": ["inference/prediction", "analysis/inferred_section"]}})
    # all objects inserted here have start_ts and end_ts and are trip-like
    del_query.update({"data.start_ts": {"$gt": reset_ts}})
    logging.debug("After all updates, del_query = %s" % del_query)

    reset_pipeline_query = {"pipeline_stage": ecwp.PipelineStages.MODE_INFERENCE.value}
    # Fuzz the TRIP_SEGMENTATION stage 5 mins because of
    # https://github.com/e-mission/e-mission-server/issues/333#issuecomment-312730217
    FUZZ_FACTOR = 5 * 60
    reset_pipeline_update = {'$set': {'last_processed_ts': reset_ts + FUZZ_FACTOR}}
    logging.info("About to reset stage %s to %s" 
        % (ecwp.PipelineStages.MODE_INFERENCE, reset_ts))
    

    logging.info("About to delete %d entries" 
        % edb.get_analysis_timeseries_db().find(del_query).count())
    logging.info("About to delete entries with keys %s" 
        % edb.get_analysis_timeseries_db().find(del_query).distinct("metadata.key"))
    
    if is_dry_run:
        logging.info("this is a dry-run, returning from del_objects_after without modifying anything")
    else:
        result = edb.get_analysis_timeseries_db().remove(del_query)
        logging.info("this is not a dry-run, result of deleting analysis entries is %s" % result)

class ModeInferencePipeline:
  def __init__(self):
    self.featureLabels = ["distance", "duration", "first filter mode", "sectionId", "avg speed",
                          "speed EV", "speed variance", "max speed", "max accel", "isCommute",
                          "heading change rate", "stop rate", "velocity change rate",
                          "start lat", "start lng", "stop lat", "stop lng",
                          "start hour", "end hour", "close to bus stop", "close to train stop",
                          "close to airport"]
    self.last_section_done = None
    with open("emission/analysis/classification/inference/mode/mode_id_old2new.txt") as fp:
        self.seed_modes_mapping = json.load(fp)
    logging.debug("Loaded modes %s" % self.seed_modes_mapping)

  def getLastSectionDone(self):
    return self.last_section_done

  # At this point, none of the clients except for CCI are supporting ground
  # truth, and even cci is only supporting trip-level ground truth. So this
  # version of the pipeline will just load a previously created model, that was
  # created from the small store of data that we do have ground truth for, and
  # we documented to have ~ 70% accuracy in the 2014 e-mission paper.

  def runPredictionPipeline(self, user_id, timerange):
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

    self.loadModelStage()
    logging.info("loadModelStage DONE")
    self.selFeatureIndices = self.selectFeatureIndicesStep()
    logging.info("selectFeatureIndicesStep DONE")
    (self.toPredictFeatureMatrix, self.tripIds, self.sectionIds) = \
        self.generateFeatureMatrixAndIDsStep(self.toPredictSections)
    logging.info("generateFeatureMatrixAndIDsStep DONE")
    self.predictedProb = self.predictModesStep()
    #This is a matrix of the entries and their corresponding probabilities for each classification
    logging.info("predictModesStep DONE")
    self.savePredictionsStep()
    logging.info("savePredictionsStep DONE")

  def loadModelStage(self):
    # TODO: Consider removing this import by moving the model save/load code to
    # its own module so that we can eventually remove the old pipeline code
    import emission.analysis.classification.inference.mode.seed.pipeline as seedp
    self.model = seedp.ModeInferencePipelineMovesFormat.loadModel()

# Features are:
# 0. distance
# 1. duration
# 2. first filter mode
# 3. sectionId
# 4. avg speed
# 5. speed EV
# 6. speed variance
# 7. max speed
# 8. max accel
# 9. isCommute
# 10. heading change rate (currently unfilled)
# 11. stop rate (currently unfilled)
# 12. velocity change rate (currently unfilled)
# 13. start lat
# 14. start lng
# 15. stop lat
# 16. stop lng
# 17. start hour
# 18. end hour
# 19. both start and end close to bus stop
# 20. both start and end close to train station
# 21. both start and end close to airport
  def updateFeatureMatrixRowWithSection(self, featureMatrix, i, section_entry):
    section = section_entry.data
    featureMatrix[i, 0] = section.distance
    featureMatrix[i, 1] = section.duration
  
    featureMatrix[i, 2] = section.sensed_mode.value
    # TODO: Figure out if I can get the section id from the new style sections
    # featureMatrix[i, 3] = section['_id']
    featureMatrix[i, 4] = easf.calOverallSectionSpeed(section)

    speeds = section['speeds']
 
    if speeds is not None and len(speeds) > 0:
        featureMatrix[i, 5] = np.mean(speeds)
        featureMatrix[i, 6] = np.std(speeds)
        featureMatrix[i, 7] = np.max(speeds)
    else:
        # They will remain zero
        pass

    accels = easf.calAccels(section)
    if accels is not None and len(accels) > 0:
      featureMatrix[i, 8] = np.max(accels)
    else:
        # They will remain zero
        pass

    featureMatrix[i, 9] = False
    featureMatrix[i, 10] = easf.calHCR(section_entry)
    featureMatrix[i, 11] = easf.calSR(section_entry)
    featureMatrix[i, 12] = easf.calVCR(section_entry)
    if 'start_loc' in section and section['end_loc'] != None:
        startCoords = section['start_loc']['coordinates']
        featureMatrix[i, 13] = startCoords[0]
        featureMatrix[i, 14] = startCoords[1]
    
    if 'end_loc' in section and section['end_loc'] != None:
        endCoords = section['end_loc']['coordinates']
        featureMatrix[i, 15] = endCoords[0]
        featureMatrix[i, 16] = endCoords[1]
    
    featureMatrix[i, 17] = section['start_local_dt']['hour']
    featureMatrix[i, 18] = section['end_local_dt']['hour']
   
    if (hasattr(self, "bus_cluster")): 
        featureMatrix[i, 19] = easf.mode_start_end_coverage(section, self.bus_cluster,105)
    if (hasattr(self, "train_cluster")): 
        featureMatrix[i, 20] = easf.mode_start_end_coverage(section, self.train_cluster,600)
    if (hasattr(self, "air_cluster")): 
        featureMatrix[i, 21] = easf.mode_start_end_coverage(section, self.air_cluster,600)

    if self.last_section_done is None or self.last_section_done.data.end_ts < section_entry.data.end_ts:
      self.last_section_done = section_entry

    # Replace NaN and inf by zeros so that it doesn't crash later
    featureMatrix[i] = np.nan_to_num(featureMatrix[i])

# Feature Indices
  def selectFeatureIndicesStep(self):
    genericFeatureIndices = list(range(0,2)) + list(range(4,9))
    AdvancedFeatureIndices = list(range(10,13))
    LocationFeatureIndices = list(range(13,17))
    TimeFeatureIndices = list(range(17,19))
    BusTrainFeatureIndices = list(range(19,22))
    logging.debug("generic features = %s" % genericFeatureIndices)
    logging.debug("advanced features = %s" % AdvancedFeatureIndices)
    logging.debug("location features = %s" % LocationFeatureIndices)
    logging.debug("time features = %s" % TimeFeatureIndices)
    logging.debug("bus train features = %s" % BusTrainFeatureIndices)
    retIndices = genericFeatureIndices
    if eac.get_config()["classification.inference.mode.useAdvancedFeatureIndices"]:
        retIndices = retIndices + AdvancedFeatureIndices
    if eac.get_config()["classification.inference.mode.useBusTrainFeatureIndices"]:
        retIndices = retIndices + BusTrainFeatureIndices
    return retIndices

  def generateFeatureMatrixAndIDsStep(self, toPredictSections):
    toPredictSections = toPredictSections
    numsections = len(toPredictSections)

    logging.debug("Predicting values for %d sections" % numsections)
    featureMatrix = np.zeros([numsections, len(self.featureLabels)])
    sectionIds = []
    tripIds = []
    for (i, section) in enumerate(toPredictSections):
      if i % 50 == 0:
        logging.debug("Processing test record %s " % i)
      self.updateFeatureMatrixRowWithSection(featureMatrix, i, section)
      sectionIds.append(section['_id'])
      tripIds.append(section.data.trip_id)

    return (featureMatrix[:,self.selFeatureIndices], tripIds, sectionIds)

  def predictModesStep(self):
    return self.model.predict_proba(self.toPredictFeatureMatrix)

  # The current probability will only have results for values from the set of
  # unique values in the resultVector. This means that the location of the
  # highest probability is not a 1:1 mapping to the mode, which will probably
  # have issues down the road. We are going to fix this here by storing the
  # non-zero probabilities in a map instead of in a list. We used to have an
  # list here, but we move to a map instead because we plan to support lots of
  # different modes, and having an giant array consisting primarily of zeros
  # doesn't sound like a great option.
  # In other words, uniqueModes = [1, 5]
  # predictedProb = [[1,0], [0,1]]
  # allModes has length 8
  # returns [{'walking': 1}, {'bus': 1}]
  def convertPredictedProbToMap(self, uniqueModes, predictedProbArr):
      currProbMap = {}
      uniqueModesInt = [int(um) for um in uniqueModes]
      logging.debug("predictedProbArr has %s non-zero elements" % np.count_nonzero(predictedProbArr))
      logging.debug("uniqueModes are %s " % uniqueModesInt)
      logging.debug("predictedProbArr = %s" % predictedProbArr)
      for (j, oldMode) in enumerate(uniqueModesInt):
        if predictedProbArr[j] != 0:
          uniqueMode = self.seed_modes_mapping[str(oldMode)]
          modeName = ecwm.PredictedModeTypes(uniqueMode).name
          logging.debug("Setting probability of mode %s (%s) to %s" %
            (uniqueMode, modeName, predictedProbArr[j]))
          currProbMap[modeName] = predictedProbArr[j]
          # logging.debug("after setting value = %s" % currProbMap[modeName])
          # logging.debug("after setting map = %s" % currProbMap)
      # logging.debug("Returning map %s" % currProbMap)
      return currProbMap

  def savePredictionsStep(self):
    from emission.core.wrapper.user import User
    from emission.core.wrapper.client import Client

    uniqueModes = self.model.classes_

    for i in range(self.predictedProb.shape[0]):
        currSectionEntry = self.toPredictSections[i]
        currSection = currSectionEntry.data
        currProb = self.convertPredictedProbToMap(uniqueModes, self.predictedProb[i])

        # Special handling for the AIR mode
        # AIR is not a mode that is sensed from the phone, but it is inferred
        # through some heuristics in cleanAndResample instead of through the
        # decision tree. Ideally those heurstics should be replaced by the
        # inference through the decision tree, or through a separate heuristic
        # step. But we are out of time for a bigger refactor here.
        # so we say that if the sensed mode == AIR, we are going to use it
        # directly and ignore the inferred mode
        if currSection.sensed_mode == ecwma.MotionTypes.AIR_OR_HSR:
            currProb = {'AIR_OR_HSR': 1.0}

        # Insert the prediction
        mp = ecwm.Modeprediction()
        mp.trip_id = currSection.trip_id
        mp.section_id = currSectionEntry.get_id()
        mp.algorithm_id = ecwm.AlgorithmTypes.SEED_RANDOM_FOREST
        mp.predicted_mode_map = currProb
        mp.start_ts = currSection.start_ts
        mp.end_ts = currSection.end_ts
        self.ts.insert_data(self.user_id, "inference/prediction", mp)

        # Since there is currently only one prediction, create the inferred
        # section object right here
        is_dict = copy.copy(currSectionEntry)
        del is_dict["_id"]
        is_dict["metadata"]["key"] = "analysis/inferred_section"
        is_dict["data"]["sensed_mode"] = ecwm.PredictedModeTypes[easf.select_inferred_mode([mp])].value
        is_dict["data"]["cleaned_section"] = currSectionEntry.get_id()
        ise = ecwe.Entry(is_dict)
        logging.debug("Updating sensed mode for section = %s to %s" % 
            (currSectionEntry.get_id(), ise.data.sensed_mode))
        self.ts.insert(ise)

if __name__ == "__main__":
  import json

  config_data = json.load(open('config.json'))
  log_base_dir = config_data['paths']['log_base_dir']
  logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                      filename="%s/pipeline.log" % log_base_dir, level=logging.DEBUG)
  modeInferPipeline = ModeInferencePipeline()
  modeInferPipeline.runPipeline()
