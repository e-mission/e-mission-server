from __future__ import print_function
from pymongo import MongoClient
import json
import sys
import emission.core.get_database as edb
from emission.tests import common

def purgeAnalysisData():
  print(edb.get_analysis_timeseries_db().remove())
  print(edb.get_common_place_db().remove())
  print(edb.get_common_trip_db().remove())
  print(edb.get_pipeline_state_db().remove())

if __name__ == '__main__':
  if len(sys.argv) == 0:
    print("USAGE: %s [userName]" % sys.argv[0])
    exit(1)

  purgeAnalysisData()
