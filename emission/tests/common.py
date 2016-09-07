# Standard imports
import logging
from datetime import datetime, timedelta
import json
import bson.json_util as bju
import uuid
import pymongo

# Our imports
from emission.core.get_database import get_client_db, get_db, get_section_db
import emission.core.get_database as edb

import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.storage.timeseries.format_hacks.move_filter_field as estfm
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.intake.cleaning.clean_and_resample as eaicr

def makeValid(client):
  client.clientJSON['start_date'] = str(datetime.now() + timedelta(days=-2))
  client.clientJSON['end_date'] = str(datetime.now() + timedelta(days=+2))
  # print (client.clientJSON)
  client._Client__update(client.clientJSON)

def makeExpired(client):
  client.clientJSON['start_date'] = str(datetime.now() + timedelta(days=-4))
  client.clientJSON['end_date'] = str(datetime.now() + timedelta(days=-2))
  # print (client.clientJSON)
  client._Client__update(client.clientJSON)

def updateUserCreateTime(uuid):
  from emission.core.wrapper.user import User
  
  user = User.fromUUID(uuid)
  user.changeUpdateTs(timedelta(days = -20))

def dropAllCollections(db):
  collections = db.collection_names()
  print "collections = %s" % collections
  for coll in collections:
    if coll.startswith('system'):
      print "Skipping system collection %s" % coll
    else: 
      print "Dropping collection %s" % coll
      db.drop_collection(coll)

def purgeSectionData(Sections, userName):
    """
    Deletes all sections for this user.
    TODO: Need to extend it to delete entries across all collections
    """
    Sections.remove({'user_id' : userName})

def loadTable(serverName, tableName, fileName):
  tableColl = get_db()[tableName]
  dataJSON = json.load(open(fileName))
  for row in dataJSON:
    tableColl.insert(row)

# Create a dummy section with the main stuff that we use in our code
def createDummySection(startTime, endTime, startLoc, endLoc, predictedMode = None, confirmedMode = None):
  section = {
             'source': 'Shankari',
             'section_start_datetime': startTime,
             'section_end_datetime': endTime,
             'section_start_time': startTime.isoformat(),
             'section_end_time': endTime.isoformat(),
             'section_start_point': {'type': 'Point', 'coordinates': startLoc},
             'section_end_point': {'type': 'Point', 'coordinates': endLoc},
            }
  if predictedMode != None:
    section['predicted_mode'] = predictedMode
  if confirmedMode != None:
    section['confirmed_mode'] = confirmedMode

  get_section_db().insert(section)
  return section

def updateSections(testCase):
    from emission.core.wrapper.user import User
    """
    Updates sections with appropriate test data
    Should be called anytime new data is loaded into the
    'Stage_Sections' table
    """
    testCase.uuid_list = []
    for section in testCase.SectionsColl.find():
      section['section_start_datetime'] = testCase.dayago
      section['section_end_datetime'] = testCase.dayago + timedelta(hours = 1)
      section['predicted_mode'] = [0, 0.4, 0.6, 0]
      section['confirmed_mode'] = ''
      # Replace the user email with the UUID
      curr_uuid = User.fromEmail(section['user_id']).uuid
      section['user_id'] = curr_uuid
      testCase.uuid_list.append(curr_uuid)
      testCase.SectionsColl.save(section)

def setupRealExample(testObj, dump_file):
    logging.info("Before loading, timeseries db size = %s" % edb.get_timeseries_db().count())
    testObj.entries = json.load(open(dump_file), object_hook = bju.object_hook)
    testObj.testUUID = uuid.uuid4()
    setupRealExampleWithEntries(testObj)

def setupRealExampleWithEntries(testObj):
    tsdb = edb.get_timeseries_db()
    for entry in testObj.entries:
        entry["user_id"] = testObj.testUUID
        # print "Saving entry with write_ts = %s and ts = %s" % (entry["metadata"]["write_fmt_time"],
        #                                                        entry["data"]["fmt_time"])
        tsdb.save(entry)
        
    logging.info("After loading, timeseries db size = %s" % edb.get_timeseries_db().count())
    logging.debug("First few entries = %s" % 
                    [e["data"]["fmt_time"] for e in 
                        list(edb.get_timeseries_db().find({"user_id": testObj.testUUID}).sort("data.write_ts",
                                                                                       pymongo.ASCENDING).limit(10))])
def runIntakePipeline(uuid):
    eaicf.filter_accuracy(uuid)
    estfm.move_all_filters_to_data()
    eaist.segment_current_trips(uuid)
    eaiss.segment_current_sections(uuid)
    eaicl.filter_current_sections(uuid)
    eaicr.clean_and_resample(uuid)

def configLogging():
    """
    Standard function to be called from the test cases to turn on logging.
    We really want the tests to configure logging in their main method so
    that individual tests can be run when they fail. But we also want a standard
    method that we can change quickly and easily.

    This is a simple way to meet both requirements.

    :return: None
    """
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(thread)d:%(message)s',
                    level=logging.DEBUG)

