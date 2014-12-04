import logging
import time
from get_database import get_client_stats_db, get_server_stats_db, get_result_stats_db

STAT_TRIP_MGR_PCT_SHOWN = "tripManager.pctShown"
STAT_MY_CARBON_FOOTPRINT = "footprint.my_carbon"
STAT_MY_CARBON_FOOTPRINT_NO_AIR = "footprint.my_carbon.no_air"
STAT_MY_OPTIMAL_FOOTPRINT = "footprint.optimal"
STAT_MY_OPTIMAL_FOOTPRINT_NO_AIR = "footprint.optimal.no_air"
STAT_MY_ALLDRIVE_FOOTPRINT = "footprint.alldrive"
STAT_MEAN_FOOTPRINT = "footprint.mean"
STAT_MEAN_FOOTPRINT_NO_AIR = "footprint.mean.no_air"
STAT_GAME_SCORE = "game.score"
STAT_VIEW_CHOICE = "view.choice"

# Store client measurements (currently into the database, but maybe in a log
# file in the future). The format of the stats received from the client is very
# similar to the input to SMAP, to make it easier to store them in a SMAP
# database in the future
def setClientMeasurements(user, reportedVals):
  logging.info("Received %d client keys and %d client readings for user %s" % (len(reportedVals['Readings']),
    getClientMeasurementCount(reportedVals['Readings']), user))
  logging.debug("reportedVals = %s" % reportedVals)
  metadata = reportedVals['Metadata']
  metadata['reported_ts'] = time.time()
  stats = reportedVals['Readings']
  for key in stats:
    values = stats[key]
    for value in values:
      storeEntry(user, key, value[0], value[1], metadata)

def storeClientEntry(user, key, ts, reading, metadata):
  logging.debug("storing client entry for user %s, key %s at timestamp %s" % (user, key, ts))
  currEntry = createEntry(user, key, ts, reading)
  # Add the os and app versions from the metadata dict
  currEntry.update(metadata)
  get_client_stats_db().insert(currEntry)

# server measurements will call this directly since there's not much point in
# batching in a different location and then making a call here since it runs on
# the same server. Might change if we move engagement stats to a different server
# Note also that there is no server metadata since we currently have no
# versioning on the server. Should probably add some soon
def storeServerEntry(user, key, ts, reading):
  logging.debug("storing server entry %s for user %s, key %s at timestamp %s" % (reading, user, key, ts))
  currEntry = createEntry(user, key, ts, reading)
  get_server_stats_db().insert(currEntry)

def storeResultEntry(user, key, ts, reading):
  logging.debug("storing result entry %s for user %s, key %s at timestamp %s" % (reading, user, key, ts))
  currEntry = createEntry(user, key, ts, reading)
  get_result_stats_db().insert(currEntry)

def getClientMeasurementCount(readings):
  retSum = 0
  for currReading in readings:
    currArray = readings[currReading]
    # logging.debug("currArray for reading %s is %s and its length is %d" % (currReading, currArray, len(currArray)))
    retSum = retSum + len(currArray)
  return retSum

def createEntry(user, stat, ts, reading):
  return {'user': user,
          'stat': stat,
          'client_ts': ts,
          'reading': reading}
