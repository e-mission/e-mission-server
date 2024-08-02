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
import emission.core.get_database as edb
import emission.tests.common as etc
import emission.storage.pipeline_queries as espq
import emission.pipeline.purge_stage as epp
import emission.pipeline.restore_stage as epr
import emission.purge_restore.export_timeseries as epret
import emission.purge_restore.purge_data as eprpd
import bin.debug.load_multi_timeline_for_range as lmtfr
import logging

class TestPurgeRestoreModule(unittest.TestCase):
    def setUp(self):
        self.testEmail = "testPurgeRestoreUser123"
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")
        print("Test UUID for Purge: %s" % self.testUUID)
        etc.runIntakePipeline(self.testUUID)

    def tearDown(self):
        print("Clearing entries for test UUID from database...")
        # etc.dropAllCollections(edb._get_current_db())
        # self.clearRelatedDb()
        # self.clearAllDb()

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
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        time_query = {
            'startTs': 1437578093.881,
            'endTs': 1437633635.069 # Exporting all 1906 entries
            # 'endTs': 1437597615.778 # Exporting first 650 entries
        }
        file_name = os.environ.get('DATA_DIR', 'emission/archived') + "/archive_%s_%s_%s" % (self.testUUID, time_query['startTs'], time_query['endTs'])

        export_queries = epret.export(self.testUUID, ts, time_query['startTs'], time_query['endTs'], file_name, False)
        pdp = eprpd.PurgeDataPipeline()
        pdp.export_pipeline_states(self.testUUID, file_name)

        '''
        Test 1 - Assert the file exists after the export process
        '''
        self.assertTrue(pl.Path(file_name + ".gz").is_file()) 
        with gzip.open(file_name + ".gz", 'r') as ef:
            exported_data = json.loads(ef.read().decode('utf-8'))

        '''
        Test 2 - Verify that purging timeseries data works with sample real data
        '''
        # Check how much data there was before
        res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
        logging.info(f"About to purge {res} entries")
        self.assertEqual(res, 1906)

        pdp.delete_timeseries_entries(self.testUUID, ts, time_query['startTs'], time_query['endTs'], export_queries)

        # Check how much data there is after
        res = res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
        logging.info(f"Purging complete: {res} entries remaining")
        self.assertEqual(res, 0)

        '''
        Test 3 - Verify that restoring timeseries data works with sample real data
        '''
        # Run the restore function
        logging.info(f"About to restore entries")
        lmtfr.load_multi_timeline_for_range(file_prefix=file_name, continue_on_error=True)

        # Check how much data there is after
        res = res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
        logging.info(f"Restoring complete: {res} entries restored")
        self.assertEqual(res, 1906)

    def testPurgeRestorePipeline(self):
        file_name = epp.run_purge_pipeline_for_user(self.testUUID, os.environ.get('DATA_DIR', 'emission/archived'))
        epr.run_restore_pipeline_for_user(self.testUUID, file_name)


if __name__ == '__main__':
     etc.configLogging()
     unittest.main()
