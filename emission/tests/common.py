from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import *
import logging
from datetime import datetime, timedelta
import json
import bson.json_util as bju
import uuid
import pymongo

# Our imports
import emission.core.get_database as edb
from emission.core.get_database import get_client_db, get_section_db
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
  print("collections = %s" % collections)
  for coll in collections:
    if coll.startswith('system'):
      print("Skipping system collection %s" % coll)
    else: 
      print("Dropping collection %s" % coll)
      db.drop_collection(coll)

def purgeSectionData(Sections, userName):
    """
    Deletes all sections for this user.
    TODO: Need to extend it to delete entries across all collections
    """
    Sections.delete_many({'user_id' : userName})

def loadTable(serverName, tableName, fileName):
  tableColl = edb._get_current_db()[tableName]
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
    with open(dump_file) as dfp:
        testObj.entries = json.load(dfp, object_hook = bju.object_hook)
        testObj.testUUID = uuid.uuid4()
        print("Setting up real example for %s" % testObj.testUUID)
        setupRealExampleWithEntries(testObj)

def setupRealExampleWithEntries(testObj):
    tsdb = edb.get_timeseries_db()
    for entry in testObj.entries:
        entry["user_id"] = testObj.testUUID
        # print "Saving entry with write_ts = %s and ts = %s" % (entry["metadata"]["write_fmt_time"],
        #                                                        entry["data"]["fmt_time"])
        edb.save(tsdb, entry)
        
    logging.info("After loading, timeseries db size = %s" % edb.get_timeseries_db().count())
    logging.debug("First few entries = %s" % 
                    [e["data"]["fmt_time"] if "fmt_time" in e["data"] else e["metadata"]["write_fmt_time"] for e in 
                        list(edb.get_timeseries_db().find({"user_id": testObj.testUUID}).sort("data.write_ts",
                                                                                       pymongo.ASCENDING).limit(10))])
def runIntakePipeline(uuid):
    eaicf.filter_accuracy(uuid)
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

def setupTokenListAuth(self):
    token_list_conf_file = open(self.token_list_conf_path, "w")
    token_list_conf_json = {
        "token_list": self.token_list_path
    }

    token_list_conf_file.write(str(json.dumps(token_list_conf_json)))
    token_list_file = open(self.token_list_path, "w")
    token_list_file.write("correct_horse_battery_staple\n")
    token_list_file.write("collar_highly_asset_ovoid_sultan\n")
    token_list_file.write("caper_hangup_addle_oboist_scroll\n")
    token_list_file.write("couple_honcho_abbot_obtain_simple\n")

def tearDownTokenListAuth(self):
    import os

    os.remove(self.token_list_conf_path)
    os.remove(self.token_list_path)

def createDummyRequestEnviron(self, addl_headers, request_body):
    # request_body is a StringIO object
    test_environ = {'HTTP_REFERER': 'http://localhost:8080/',
        'SERVER_SOFTWARE': 'CherryPy/3.6.0 Server',
        'SCRIPT_NAME': '',
        'ACTUAL_SERVER_PROTOCOL': 'HTTP/1.1',
        'REQUEST_METHOD': 'POST',
        'PATH_INFO': '/result/heatmap/pop.route/local_date',
        'SERVER_PROTOCOL': 'HTTP/1.1',
        'QUERY_STRING': '',
        'bottle.request.body': request_body,
        'CONTENT_TYPE': 'application/json;charset=utf-8',
        'wsgi.input': request_body,
        'wsgi.multithread': True,
        'HTTP_ACCEPT_LANGUAGE': 'en-US,en;q=0.5',
        'HTTP_ACCEPT_ENCODING': 'gzip, deflate'
    }
    if addl_headers is not None:
        test_environ.update(addl_headers)
    return test_environ
