# Standard imports
from pymongo import MongoClient
import logging
from datetime import datetime
import sys
import os
import numpy as np
import scipy as sp
import time
from datetime import datetime

# Our imports
import emission.analysis.section_features as easf
import emission.core.get_database as edb

# We are not going to use the feature matrix for analysis unless we have at
# least 50 points in the training set. 50 is arbitrary. We could also consider
# combining the old and new training data, but this is really a bootstrapping
# problem, so we don't need to solve it right now.
minTrainingSetSize = 1000

class ModeInferencePipeline:
  def __init__(self):
    self.featureLabels = ["distance", "duration", "first filter mode", "sectionId", "avg speed",
                          "speed EV", "speed variance", "max speed", "max accel", "isCommute",
                          "heading change rate", "stop rate", "velocity change rate",
                          "start lat", "start lng", "stop lat", "stop lng",
                          "start hour", "end hour", "close to bus stop", "close to train stop",
                          "close to airport"]
    self.Sections = edb.get_section_db()

  def runPipeline(self):
    allConfirmedTripsQuery = ModeInferencePipeline.getSectionQueryWithGroundTruth({'$ne': ''})



    (self.modeList, self.confirmedSections) = self.loadTrainingDataStep(allConfirmedTripsQuery)
    logging.debug("confirmedSections.count() = %s" % (self.confirmedSections.count()))
    
    if (self.confirmedSections.count() < minTrainingSetSize):
      logging.info("initial loadTrainingDataStep DONE")
      logging.debug("current training set too small, reloading from backup!")
      backupSections = MongoClient('localhost').Backup_database.Stage_Sections
      (self.modeList, self.confirmedSections) = self.loadTrainingDataStep(allConfirmedTripsQuery, backupSections)
    logging.info("loadTrainingDataStep DONE")
    (self.bus_cluster, self.train_cluster) = self.generateBusAndTrainStopStep() 
    logging.info("generateBusAndTrainStopStep DONE")
    (self.featureMatrix, self.resultVector) = self.generateFeatureMatrixAndResultVectorStep()
    logging.info("generateFeatureMatrixAndResultVectorStep DONE")
    (self.cleanedFeatureMatrix, self.cleanedResultVector) = self.cleanDataStep()
    logging.info("cleanDataStep DONE")
    self.selFeatureIndices = self.selectFeatureIndicesStep()
    logging.info("selectFeatureIndicesStep DONE")
    self.selFeatureMatrix = self.cleanedFeatureMatrix[:,self.selFeatureIndices]
    self.model = self.buildModelStep()
    logging.info("buildModelStep DONE")
    toPredictTripsQuery = {"$and": [{'type': 'move'},
                                    ModeInferencePipeline.getModeQuery(''),
                                    {'predicted_mode': None}]}
    (self.toPredictFeatureMatrix, self.sectionIds, self.sectionUserIds) = self.generateFeatureMatrixAndIDsStep(toPredictTripsQuery)
    logging.info("generateFeatureMatrixAndIDsStep DONE")
    self.predictedProb = self.predictModesStep()
    logging.info("predictModesStep DONE")
    self.savePredictionsStep()
    logging.info("savePredictionsStep DONE")

    # Most of the time, this will be an int, but it can also be a subquery, like
    # {'$ne': ''}. This will be used to find the set of entries for the training
    # set, for example
  @staticmethod
  def getModeQuery(groundTruthMode):
    # We need the existence check because the corrected mode is not guaranteed to exist,
    # and if it doesn't exist, it will end up match the != '' query (since it
    # is not '', it is non existent)
    correctedModeQuery = lambda mode: {'$and': [{'corrected_mode': {'$exists': True}},
                                                {'corrected_mode': groundTruthMode}]}
    return {'$or': [correctedModeQuery(groundTruthMode),
                              {'confirmed_mode': groundTruthMode}]}

  @staticmethod
  def getSectionQueryWithGroundTruth(groundTruthMode):
    return {"$and": [{'type': 'move'},
                     ModeInferencePipeline.getModeQuery(groundTruthMode)]}

  # TODO: Refactor into generic steps and results
  def loadTrainingDataStep(self, sectionQuery, sectionDb = None):
    logging.debug("START TRAINING DATA STEP")
    if (sectionDb == None):
      sectionDb = self.Sections

    begin = time.time()
    logging.debug("Section data set size = %s" % sectionDb.find({'type': 'move'}).count())
    duration = time.time() - begin
    logging.debug("Getting dataset size took %s" % (duration))
        
    logging.debug("Querying confirmedSections %s" % (datetime.now()))
    begin = time.time()
    confirmedSections = sectionDb.find(sectionQuery).sort('_id', 1)

    duration = time.time() - begin
    logging.debug("Querying confirmedSection took %s" % (duration))
    
    logging.debug("Querying stage modes %s" % (datetime.now()))
    begin = time.time()
    modeList = []
    for mode in edb.get_mode_db().find():
        modeList.append(mode)
        logging.debug(mode)
    duration = time.time() - begin
    logging.debug("Querying stage modes took %s" % (duration))
    
    logging.debug("Section query with ground truth %s" % (datetime.now()))
    begin = time.time()
    logging.debug("Training set total size = %s" %
      sectionDb.find(ModeInferencePipeline.getSectionQueryWithGroundTruth({'$ne': ''})).count())

    for mode in modeList:
      logging.debug("%s: %s" % (mode['mode_name'],
        sectionDb.find(ModeInferencePipeline.getSectionQueryWithGroundTruth(mode['mode_id']))))
    duration = time.time() - begin
    logging.debug("Getting section query with ground truth took %s" % (duration))
    

    duration = time.time() - begin
    return (modeList, confirmedSections)

  # TODO: Should mode_cluster be in featurecalc or here?
  def generateBusAndTrainStopStep(self):
    bus_cluster=easf.mode_cluster(5,105,1)
    train_cluster=easf.mode_cluster(6,600,1)
    air_cluster=easf.mode_cluster(9,600,1)
    return (bus_cluster, train_cluster)

# Feature matrix construction
  def generateFeatureMatrixAndResultVectorStep(self):
      featureMatrix = np.zeros([self.confirmedSections.count(), len(self.featureLabels)])
      resultVector = np.zeros(self.confirmedSections.count())
      logging.debug("created data structures of size %s" % self.confirmedSections.count())
      # There are a couple of additions to the standard confirmedSections cursor here.
      # First, we read it in batches of 300 in order to avoid the 10 minute timeout
      # Our logging shows that we can process roughly 500 entries in 10 minutes

      # Second, it looks like the cursor requeries while iterating. So when we
      # first check, we get count of x, but if new entries were read (or in
      # this case, classified) while we are iterating over the cursor, we may
      # end up processing > x entries.

      # This will crash the script because we will try to access a record that
      # doesn't exist.

      # So we limit the records to the size of the matrix that we have created
      for (i, section) in enumerate(self.confirmedSections.limit(featureMatrix.shape[0]).batch_size(300)):
        try:
            self.updateFeatureMatrixRowWithSection(featureMatrix, i, section)
            resultVector[i] = self.getGroundTruthMode(section)
            if i % 100 == 0:
                logging.debug("Processing record %s " % i)
        except Exception, e:
            logging.debug("skipping section %s due to error %s " % (section, e))
      return (featureMatrix, resultVector)

  def getGroundTruthMode(self, section):
      # logging.debug("getting ground truth for section %s" % section)
      if 'corrected_mode' in section:
          # logging.debug("Returning corrected mode %s" % section['corrected_mode'])
          return section['corrected_mode']
      else:
          # logging.debug("Returning confirmed mode %s" % section['confirmed_mode'])
          return section['confirmed_mode']

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
  def updateFeatureMatrixRowWithSection(self, featureMatrix, i, section):
    featureMatrix[i, 0] = section['distance']
    featureMatrix[i, 1] = (section['section_end_datetime'] - section['section_start_datetime']).total_seconds()

    # Deal with unknown modes like "airplane"
    try:
      featureMatrix[i, 2] = section['mode']
    except ValueError:
      featureMatrix[i, 2] = 0

    featureMatrix[i, 3] = section['section_id']
    featureMatrix[i, 4] = easf.calAvgSpeed(section)
    speeds = easf.calSpeeds(section)
    if speeds != None and len(speeds) > 0:
        featureMatrix[i, 5] = np.mean(speeds)
        featureMatrix[i, 6] = np.std(speeds)
        featureMatrix[i, 7] = np.max(speeds)
    else:
        # They will remain zero
        pass
    accels = easf.calAccels(section)
    if accels != None and len(accels) > 0:
        featureMatrix[i, 8] = np.max(accels)
    else:
        # They will remain zero
        pass
    featureMatrix[i, 9] = ('commute' in section) and (section['commute'] == 'to' or section['commute'] == 'from')
    featureMatrix[i, 10] = easf.calHCR(section)
    featureMatrix[i, 11] = easf.calSR(section)
    featureMatrix[i, 12] = easf.calVCR(section)
    if 'section_start_point' in section and section['section_start_point'] != None:
        startCoords = section['section_start_point']['coordinates']
        featureMatrix[i, 13] = startCoords[0]
        featureMatrix[i, 14] = startCoords[1]
    
    if 'section_end_point' in section and section['section_end_point'] != None:
        endCoords = section['section_end_point']['coordinates']
        featureMatrix[i, 15] = endCoords[0]
        featureMatrix[i, 16] = endCoords[1]
    
    featureMatrix[i, 17] = section['section_start_datetime'].time().hour
    featureMatrix[i, 18] = section['section_end_datetime'].time().hour
   
    if (hasattr(self, "bus_cluster")): 
        featureMatrix[i, 19] = easf.mode_start_end_coverage(section, self.bus_cluster,105)
    if (hasattr(self, "train_cluster")): 
        featureMatrix[i, 20] = easf.mode_start_end_coverage(section, self.train_cluster,600)
    if (hasattr(self, "air_cluster")): 
        featureMatrix[i, 21] = easf.mode_start_end_coverage(section, self.air_cluster,600)

    # Replace NaN and inf by zeros so that it doesn't crash later
    featureMatrix[i] = np.nan_to_num(featureMatrix[i])

  def cleanDataStep(self):
    runIndices = self.resultVector == 2
    transportIndices = self.resultVector == 4
    mixedIndices = self.resultVector == 8
    airIndices = self.resultVector == 9
    unknownIndices = self.resultVector == 0
    strippedIndices = np.logical_not(runIndices | transportIndices | mixedIndices | unknownIndices)
    logging.debug("Stripped trips with mode: run %s, transport %s, mixed %s, unknown %s unstripped %s" %
      (np.count_nonzero(runIndices), np.count_nonzero(transportIndices),
      np.count_nonzero(mixedIndices), np.count_nonzero(unknownIndices),
      np.count_nonzero(strippedIndices)))

    strippedFeatureMatrix = self.featureMatrix[strippedIndices]
    strippedResultVector = self.resultVector[strippedIndices]

    # In spite of stripping out the values, we see that there are clear
    # outliers. This is almost certainly a mis-classified trip, because the
    # distance and speed are both really large, but the mode is walking. Let's
    # manually filter out this outlier.

    distanceOutliers = strippedFeatureMatrix[:,0] > 500000
    speedOutliers = strippedFeatureMatrix[:,4] > 100
    speedMeanOutliers = strippedFeatureMatrix[:,5] > 80
    speedVarianceOutliers = strippedFeatureMatrix[:,6] > 70
    maxSpeedOutliers = strippedFeatureMatrix[:,7] > 160
    logging.debug("Stripping out distanceOutliers %s, speedOutliers %s, speedMeanOutliers %s, speedVarianceOutliers %s, maxSpeedOutliers %s" % 
            (np.nonzero(distanceOutliers), np.nonzero(speedOutliers),
            np.nonzero(speedMeanOutliers), np.nonzero(speedVarianceOutliers),
            np.nonzero(maxSpeedOutliers)))
    nonOutlierIndices = np.logical_not(distanceOutliers | speedOutliers | speedMeanOutliers | speedVarianceOutliers | maxSpeedOutliers)
    logging.debug("nonOutlierIndices.shape = %s" % nonOutlierIndices.shape)

    return (strippedFeatureMatrix[nonOutlierIndices],
            strippedResultVector[nonOutlierIndices])

# Feature Indices
  def selectFeatureIndicesStep(self):
    genericFeatureIndices = list(xrange(0,10))
    AdvancedFeatureIndices = list(xrange(10,13))
    LocationFeatureIndices = list(xrange(13,17))
    TimeFeatureIndices = list(xrange(17,19))
    BusTrainFeatureIndices = list(xrange(19,22))
    logging.debug("generic features = %s" % genericFeatureIndices)
    logging.debug("advanced features = %s" % AdvancedFeatureIndices)
    logging.debug("location features = %s" % LocationFeatureIndices)
    logging.debug("time features = %s" % TimeFeatureIndices)
    logging.debug("bus train features = %s" % BusTrainFeatureIndices)
    return genericFeatureIndices + BusTrainFeatureIndices

  def buildModelStep(self):
    from sklearn import ensemble
    forestClf = ensemble.RandomForestClassifier()
    model = forestClf.fit(self.selFeatureMatrix, self.cleanedResultVector)
    return model

  def generateFeatureMatrixAndIDsStep(self, sectionQuery):
    toPredictSections = self.Sections.find(sectionQuery)
    logging.debug("Predicting values for %d sections" % toPredictSections.count())
    featureMatrix = np.zeros([toPredictSections.count(), len(self.featureLabels)])
    sectionIds = []
    sectionUserIds = []
    for (i, section) in enumerate(toPredictSections.limit(featureMatrix.shape[0]).batch_size(300)):
      if i % 50 == 0:
        logging.debug("Processing test record %s " % i)
      self.updateFeatureMatrixRowWithSection(featureMatrix, i, section)
      sectionIds.append(section['_id'])
      sectionUserIds.append(section['user_id'])
    return (featureMatrix[:,self.selFeatureIndices], sectionIds, sectionUserIds)

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
  def convertPredictedProbToMap(self, allModeList, uniqueModes, predictedProbArr):
      currProbMap = {}
      uniqueModesInt = [int(um) for um in uniqueModes]
      logging.debug("predictedProbArr has %s non-zero elements" % np.count_nonzero(predictedProbArr))
      logging.debug("uniqueModes are %s " % uniqueModesInt)
      for (j, uniqueMode) in enumerate(uniqueModesInt):
        if predictedProbArr[j] != 0:
          # Modes start from 1, but allModeList indices start from 0
          # so walking (mode id 1) -> modeList[0]
          modeName = allModeList[uniqueMode-1]['mode_name']
          logging.debug("Setting probability of mode %s (%s) to %s" %
            (uniqueMode, modeName, predictedProbArr[j]))
          currProbMap[modeName] = predictedProbArr[j]
      return currProbMap

  def savePredictionsStep(self):
    from emission.core.wrapper.user import User
    from emission.core.wrapper.client import Client

    uniqueModes = sorted(set(self.cleanedResultVector))

    for i in range(self.predictedProb.shape[0]):
      currSectionId = self.sectionIds[i]
      currProb = self.convertPredictedProbToMap(self.modeList, uniqueModes, self.predictedProb[i])

      logging.debug("Updating probability for section with id = %s" % currSectionId)
      self.Sections.update({'_id': currSectionId}, {"$set": {"predicted_mode": currProb}})

      currUser = User.fromUUID(self.sectionUserIds[i])
      clientSpecificUpdate = Client(currUser.getFirstStudy()).clientSpecificSetters(currUser.uuid, currSectionId, currProb)
      if clientSpecificUpdate != None:
        self.Sections.update({'_id': currSectionId}, clientSpecificUpdate)

if __name__ == "__main__":
  import json

  config_data = json.load(open('config.json'))
  log_base_dir = config_data['paths']['log_base_dir']
  logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                      filename="%s/pipeline.log" % log_base_dir, level=logging.DEBUG)
  modeInferPipeline = ModeInferencePipeline()
  modeInferPipeline.runPipeline()
