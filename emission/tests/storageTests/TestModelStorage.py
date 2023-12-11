from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import datetime as pydt
import logging
import json
import pymongo
import uuid

# Our imports
import emission.core.get_database as edb
import emission.storage.modifiable.abstract_model_storage as esma
import emission.storage.decorations.analysis_timeseries_queries as esda

# Test imports
import emission.tests.common as etc

class TestModelStorage(unittest.TestCase):
    def setUp(self):
        # Tested with stage dataset snapshot with models; working fine.
        self.user_id = uuid.UUID("a1f1c01f-d30b-43f0-bc78-b7a4d97d576a")

    def tearDown(self):
        print("Reset")

    def testTrimModelEntries(self):
        ms = esma.ModelStorage.get_model_storage(self.user_id)
        ms.trim_model_entries(key=esda.TRIP_MODEL_STORE_KEY)

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
