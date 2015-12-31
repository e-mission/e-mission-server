# Standard imports
import logging
import time

# Our imports
from emission.core.get_database import get_client_stats_db, get_server_stats_db, get_result_stats_db, get_client_stats_db_backup, get_server_stats_db_backup, get_result_stats_db_backup

STAT_TRIP_MGR_PCT_SHOWN = "tripManager.pctShown"
STAT_TRIP_MGR_TRIPS_FOR_DAY = "tripManager.tripsForDay"

STAT_MY_CARBON_FOOTPRINT = "footprint.my_carbon"
STAT_MY_CARBON_FOOTPRINT_NO_AIR = "footprint.my_carbon.no_air"
STAT_MY_OPTIMAL_FOOTPRINT = "footprint.optimal"
STAT_MY_OPTIMAL_FOOTPRINT_NO_AIR = "footprint.optimal.no_air"
STAT_MY_ALLDRIVE_FOOTPRINT = "footprint.alldrive"

STAT_PCT_CLASSIFIED = "game.score.pct_classified"
STAT_MINE_MINUS_OPTIMAL = "game.score.mine_minus_optimal"
STAT_ALL_DRIVE_MINUS_MINE = "game.score.all_drive_minus_mine"
STAT_SB375_DAILY_GOAL = "game.score.sb375_daily_goal"

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
      storeClientEntry(user, key, value[0], value[1], metadata)

# metadata format is 
def storeClientEntry(user, key, ts, reading, metadata):
  logging.debug("storing client entry for user %s, key %s at timestamp %s" % (user, key, ts))
  response = None
  # float first, because int doesn't recognize floats represented as strings.
  # Android timestamps are in milliseconds, while Giles expects timestamps to be
  # in seconds, so divide by 1000 when you hit this case.
  # ios timestamps are in seconds.
  ts = int(ts)

  if ts > 9999999999:
    ts = ts/1000

  currEntry = createEntry(user, key, ts, reading)
  # Add the os and app versions from the metadata dict
  currEntry.update(metadata)

  try:
    response = get_client_stats_db().insert(currEntry)
    if response == None:
      get_client_stats_db_backup().insert(currEntry)
  except Exception as e:
    logging.debug("failed to store client entry for user %s, key %s at timestamp %s" % (user, key, ts))
    logging.debug("exception was: %s" % (e))
    get_client_stats_db_backup().insert(currEntry)

  return response != None

# server measurements will call this directly since there's not much point in
# batching in a different location and then making a call here since it runs on
# the same server. Might change if we move engagement stats to a different server
# Note also that there is no server metadata since we currently have no
# versioning on the server. Should probably add some soon

def storeServerEntry(user, key, ts, reading):
  logging.debug("storing server entry %s for user %s, key %s at timestamp %s" % (reading, user, key, ts))
  response = None
  currEntry = createEntry(user, key, ts, reading)
  
  try:
    response = get_server_stats_db().insert(currEntry)
    if response == None:
      get_server_stats_db_backup().insert(currEntry)
     
  except Exception as e:
    logging.debug("failed to store server entry %s for user %s, key %s at timestamp %s" % (reading, user, key, ts))
    logging.debug("exception was: %s" % (e))
    get_server_stats_db_backup().insert(currEntry)


  # Return boolean that tells you whether the insertion was successful or not
  return response != None

def storeResultEntry(user, key, ts, reading):
  logging.debug("storing result entry %s for user %s, key %s at timestamp %s" % (reading, user, key, ts))
  response = None

  # Sometimes timestamp comes in as a float, represented as seconds.[somethign else]; truncate digits after the
  # decimal
  ts = int(ts)
  currEntry = createEntry(user, key, ts, reading)

  try:
    response = get_result_stats_db().insert(currEntry)
    if response == None:
      get_result_stats_db_backup().insert(currEntry)
     
  except Exception as e:
    logging.debug("failed to store result entry %s for user %s, key %s at timestamp %s" % (reading, user, key, ts))
    logging.debug("exception was: %s" % (e))
    get_result_stats_db_backup().insert(currEntry)

  # Return boolean that tells you whether the insertion was successful or not
  return response != None


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
          'ts': ts,
          'reading': reading}
