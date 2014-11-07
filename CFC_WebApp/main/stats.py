import logging
import time
from get_database import get_client_stats_db

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
      currEntry = createEntry(user, key, value[0], value[1])
      # Add the os and app versions from the metadata dict
      currEntry.update(metadata)
      get_client_stats_db().insert(currEntry)

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
