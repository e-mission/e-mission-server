from future import standard_library
standard_library.install_aliases()
from builtins import *
import os
from os import path
import tempfile
import unittest
import json
import bson.json_util as bju
import pathlib as pl
import emission.storage.timeseries.abstract_timeseries as esta
import gzip
import emission.tests.common as etc
import emission.pipeline.export_stage as epe
import emission.storage.pipeline_queries as espq
import emission.exportdata.export_data as eeed
import emission.export.export as eee
import emission.pipeline.purge_stage as epp
import emission.core.get_database as edb
import emission.pipeline.restore_stage as epr

class TestPurgeRestoreModule(unittest.TestCase):
    def setUp(self):
        self.testEmail = "testPurgeRestoreUser123"
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")
        print("Test UUID for Purge: %s" % self.testUUID)
        etc.runIntakePipeline(self.testUUID)

    def tearDown(self):
        print("Clearing entries for test UUID from database...")
        self.clearRelatedDb()
        self.clearAllDb()

    def clearRelatedDb(self):
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_usercache_db().delete_many({'user_id': self.testUUID})
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID})
        edb.get_uuid_db().delete_one({"user_email": self.testEmail})

    def clearAllDb(self):
        edb.get_timeseries_db().delete_many({})
        edb.get_analysis_timeseries_db().delete_many({})
        edb.get_usercache_db().delete_many({})
        edb.get_pipeline_state_db().delete_many({})
        edb.get_uuid_db().delete_one({})

    def testPurgeRestoreModule(self):
        file_name = epp.run_purge_pipeline_for_user(self.testUUID, os.environ.get('DATA_DIR', 'emission/archived'))
        epr.run_restore_pipeline_for_user(self.testUUID, file_name)


if __name__ == '__main__':
     etc.configLogging()
     unittest.main()
