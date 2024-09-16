from future import standard_library
standard_library.install_aliases()
from builtins import *
import os
import unittest
import json
import pathlib as pl
import gzip
import logging
import tempfile
import time
from bson.objectid import ObjectId

import emission.core.get_database as edb
import emission.tests.common as etc
import emission.storage.timeseries.abstract_timeseries as esta
import emission.pipeline.purge_stage as epp
import emission.pipeline.restore_stage as epr
import emission.purge_restore.purge_data as eprpd
import emission.storage.json_wrappers as esj
import emission.storage.timeseries.timequery as estt

class TestPurgeRestoreModule(unittest.TestCase):
    def setUp(self):
        self.testEmail = "testPurgeRestoreUser123"
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")
        logging.debug("Test UUID for Purge: %s" % self.testUUID)
        etc.runIntakePipeline(self.testUUID)

    def tearDown(self):
        self.clearAllDb()

    def clearAllDb(self):
        edb.get_timeseries_db().delete_many({})
        edb.get_analysis_timeseries_db().delete_many({})
        edb.get_usercache_db().delete_many({})
        edb.get_pipeline_state_db().delete_many({})
        edb.get_uuid_db().delete_one({})

    def getEntriesToExport(self, tmpdirname):
            self.assertTrue(os.path.isdir(tmpdirname))

            #Set the envrionment variable
            os.environ['DATA_DIR'] = tmpdirname
            self.assertEqual(os.environ['DATA_DIR'], tmpdirname)

            # Fetch entries from timeseries db before purging to use in tests
            ts = esta.TimeSeries.get_time_series(self.testUUID)
            tq = estt.TimeQuery("data.ts", None, time.time() - 5)
            sort_key = ts._get_sort_key(tq)
            (ts_db_count, ts_db_result) = ts._get_entries_for_timeseries(ts.timeseries_db, None, tq, geo_query=None, extra_query_list=None, sort_key = sort_key)
            entries_to_export = list(ts_db_result)
            return entries_to_export
    
    def prePipelineTests(self):
        '''
        Test 1 - Verify that purging timeseries data works with sample real data
        '''
        # Check how much data there was before
        res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
        logging.debug(f"About to purge {res} entries")
        self.assertEqual(res, 1906)

    def postPipelineTests(self, entries_to_export, file_names):
        logging.debug("Exported file names: %s" % file_names)
        '''
        Test 2 - Assert the file exists after the export process 
        '''
        exported_data = []
        for file_name in file_names:
            self.assertTrue(pl.Path(file_name + ".gz").is_file()) 
            with gzip.open(file_name + ".gz", 'r') as ef:
                exported_data.extend(json.loads(ef.read().decode('utf-8')))
                # exported_data = json.loads(ef.read().decode('utf-8'))
        self.assertEqual(len(exported_data), 1906)

        '''
        Test 3 - Compare the first and last few entries in the exported file with the entries in the timeseries db
        '''
        entries_from_db = entries_to_export
        logging.debug("Entries from db size: %s" % len(entries_from_db))
        entries_from_db = entries_from_db[:5] + entries_from_db[-5:]
        entries_from_file = exported_data[:5] + exported_data[-5:]
        objectIds_from_db = [entry["_id"] for entry in entries_from_db]
        objectIds_from_file = [ObjectId(entry["_id"]["$oid"]) for entry in entries_from_file]
        logging.debug("Object ids from db: %s" % objectIds_from_db)
        logging.debug("Object ids from file: %s" % objectIds_from_file)
        self.assertEqual(objectIds_from_db, objectIds_from_file)

        '''
        Test 4 - Verify that purging timeseries data works with sample real data
        '''
        # Check how much data there is after
        entries = edb.get_timeseries_db().find({"user_id" : self.testUUID})
        res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
        logging.debug(f"Purging complete: {res} entries remaining")
        
        # A single entry with key 'stats/pipeline_time' should be present as this test involves running the pipeline
        stat_pipeline_key = entries[0].get('metadata').get('key')
        logging.debug(f"stat_pipeline_key = {stat_pipeline_key}")
        self.assertEqual(stat_pipeline_key,'stats/pipeline_time')
        self.assertEqual(res, 1)

        # Run the restore pipeline
        logging.debug(f"About to restore entries")
        logging.debug("File names: %s" % file_names)
        epr.run_restore_pipeline_for_user(self.testUUID, file_names)

        '''
        Test 5 - Verify that restoring timeseries data works with sample real data
        '''
        # Check how much data there is after
        res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
        res_stats_count = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID, "metadata.key" : 'stats/pipeline_time'})
        logging.debug(f"Restoring complete: {res-2} entries restored")

        # Two additional entries with key 'stats/pipeline_time' should be present - one from the purge pipeline, other from the restore pipeline
        self.assertEqual(res_stats_count, 2)
        self.assertEqual(res, 1908)

        '''
        Test 6 - Verify that restoring timeseries data is skipped if data already exists
            Duplicate key error is ignored in import_timeseries.py
            Hence no entries should be inserted
        '''
        logging.debug("Attempting to load duplicate data...")
        epr.run_restore_pipeline_for_user(self.testUUID, file_names)
        # Check how much data there is after
        res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
        res_stats_count = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID, "metadata.key" : 'stats/pipeline_time'})
        logging.debug(f"Restoring complete: {res-2} entries restored")

        # A third entry with key 'stats/pipeline_time' should be present after running the restore pipeline again
        self.assertEqual(res_stats_count, 3)
        self.assertEqual(res, 1909)


    def testPurgeRestorePipelineFull(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as tmpdirname:
            entries_to_export = self.getEntriesToExport(tmpdirname)
            self.prePipelineTests()
            file_names = epp.run_purge_pipeline_for_user(self.testUUID, os.environ.get('DATA_DIR', 'emission/archived'), "full")
            self.postPipelineTests(entries_to_export, file_names)

    def testPurgeRestorePipelineIncremental(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as tmpdirname:
            entries_to_export = self.getEntriesToExport(tmpdirname)
            self.prePipelineTests()
            file_names = epp.run_purge_pipeline_for_user(self.testUUID, os.environ.get('DATA_DIR', 'emission/archived'), "incremental")
            self.postPipelineTests(entries_to_export, file_names)

if __name__ == '__main__':
     etc.configLogging()
     unittest.main()
