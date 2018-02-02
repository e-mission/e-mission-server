# Standard imports
import unittest
import sys
import os
from datetime import datetime, timedelta
import logging

# Our imports
from emission.core.wrapper.tiersys import TierSys
from emission.core.wrapper.user import User
from emission.tests import common
import emission.tests.common as etc
import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta
import pandas as pd
import arrow
from emission.core.get_database import get_tiersys_db
import uuid

import emission.tests.common as etc

class TestTierSys(unittest.TestCase):
  def setUp(self):
      etc.dropAllCollections(edb._get_current_db())
      return

  def testUpdatePolarBear(self):
  	""" 
	Creates a tier 
  	"""


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
