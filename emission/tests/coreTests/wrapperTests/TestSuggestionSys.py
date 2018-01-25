# Standard imports
import unittest
import sys
import os
from datetime import datetime, timedelta
import logging

# Our imports
import emission.core.wrapper.suggestion_sys as sugg_sys
import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta
import pandas as pd
import arrow
from emission.core.get_database import get_tiersys_db
import uuid

import emission.tests.common as etc

class TestSuggestionSys(unittest.TestCase):
  def setUp(self):
      etc.dropAllCollections(edb._get_current_db())
      return

  def printUUIDs(self):
      all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"user_email":1, "uuid": 1, "_id": 0})))
      for uuid in all_users["uuid"]:
          print(uuid)

      return

  def printSuggestions(self):
      all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"user_email":1, "uuid": 1, "_id": 0})))
      for uuid in all_users["uuid"]:
          print(sugg_sys.calculate_single_suggestion(uuid))
      return


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
