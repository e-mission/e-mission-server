from future import standard_library
standard_library.install_aliases()
from builtins import *
import os
import unittest
import json
import pathlib as pl
import emission.storage.timeseries.abstract_timeseries as esta
import gzip
import emission.core.get_database as edb
import emission.tests.common as etc
import emission.storage.pipeline_queries as espq
import emission.pipeline.purge_stage as epp
import emission.pipeline.restore_stage as epr
import emission.purge_restore.export_timeseries as epret
import emission.purge_restore.import_timeseries as eprit
import emission.purge_restore.purge_data as eprpd
import bin.debug.load_multi_timeline_for_range as lmtfr
import logging
import gzip
import emission.storage.json_wrappers as esj

class TestPurgeRestoreModule(unittest.TestCase):
    def setUp(self):
        self.testEmail = "testPurgeRestoreUser123"
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")
        # etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-27")
        print("Test UUID for Purge: %s" % self.testUUID)
        etc.runIntakePipeline(self.testUUID)

    def tearDown(self):
        print("Clearing entries for test UUID from database...")
        # etc.dropAllCollections(edb._get_current_db())
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

    # def testPurgeRestoreModule(self):
    #     ts = esta.TimeSeries.get_time_series(self.testUUID)
    #     time_query = {
    #         'startTs': 1437578093.881,
    #         'endTs': 1437633635.069 # Exporting all 1906 entries
    #         # 'endTs': 1437597615.778 # Exporting first 650 entries
    #     }
    #     file_name = os.environ.get('DATA_DIR', 'emission/archived') + "/archive_%s_%s_%s" % (self.testUUID, time_query['startTs'], time_query['endTs'])

    #     export_queries = epret.export(self.testUUID, ts, time_query['startTs'], time_query['endTs'], file_name, False)
    #     pdp = eprpd.PurgeDataPipeline()
    #     # pdp.export_pipeline_states(self.testUUID, file_name)

    #     '''
    #     Test 1 - Assert the file exists after the export process
    #     '''
    #     self.assertTrue(pl.Path(file_name + ".gz").is_file()) 
    #     # with gzip.open(file_name + ".gz", 'r') as ef:
    #     #     exported_data = json.loads(ef.read().decode('utf-8'))

    #     '''
    #     Test 2 - Verify that purging timeseries data works with sample real data
    #     '''
    #     # Check how much data there was before
    #     res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
    #     logging.info(f"About to purge {res} entries")
    #     print(f"About to purge {res} entries")
    #     self.assertEqual(res, 1906)

    #     pdp.delete_timeseries_entries(self.testUUID, ts, time_query['startTs'], time_query['endTs'])

    #     # Check how much data there is after
    #     res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
    #     logging.info(f"Purging complete: {res} entries remaining")
    #     print(f"Purging complete: {res} entries remaining")
    #     self.assertEqual(res, 0)

    #     '''
    #     Test 3 - Verify that restoring timeseries data works with sample real data
    #     '''
    #     # Run the restore function
    #     logging.info(f"About to restore entries")
    #     print(f"About to restore entries")
    #     (tsdb_count, ucdb_count) = eprit.load_multi_timeline_for_range(file_prefix=file_name, continue_on_error=True)
    #     # lmtfr.load_multi_timeline_for_range(file_prefix=file_name, continue_on_error=True)

    #     # Check how much data there is after
    #     res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
    #     logging.info(f"Restoring complete: {res} entries restored")
    #     print(f"Restoring complete: {res} entries restored")
    #     self.assertEqual(res, 1906)

    #     '''
    #     Test 4 - Verify that restoring timeseries data fails if data already exists
    #     Duplicate key error is ignored hence no entries should be inserted
    #     '''
    #     logging.info("Attempting to load duplicate data...")
    #     print("Attempting to load duplicate data...")
    #     (tsdb_count, ucdb_count) = eprit.load_multi_timeline_for_range(file_prefix=file_name, continue_on_error=True)
    #     self.assertEqual(tsdb_count, 0)

    def testPurgeRestorePipelineFull(self):
        '''
        Test 1 - Verify that purging timeseries data works with sample real data
        '''
        # Check how much data there was before
        res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
        logging.info(f"About to purge {res} entries")
        print(f"About to purge {res} entries")
        self.assertEqual(res, 1906)

        # Run the purge pipeline
        file_names = epp.run_purge_pipeline_for_user(self.testUUID, os.environ.get('DATA_DIR', 'emission/archived'), "full")
        print("Exported file names: %s" % file_names)

        '''
        Test 2 - Assert the file exists after the export process and checking contents
        '''
        self.assertTrue(pl.Path(file_names[0] + ".gz").is_file()) 
        with gzip.open(file_names[0] + ".gz", 'r') as ef:
            exported_data = json.loads(ef.read().decode('utf-8'))
        self.assertEqual(len(exported_data), 1906)

        first_few_objectIds = ['564e73d388f663199aabf0d2', '55afb7c67d65cb39ee976598', '55afb7c67d65cb39ee976599', '55b08d327d65cb39ee9769e1', '55afb7c67d65cb39ee97659a']
        for entry in exported_data[0:5]:
            self.assertIn(entry.get('_id').get('$oid'), first_few_objectIds)

        '''
        Test 3 - Verify that purging timeseries data works with sample real data
        '''
        # Check how much data there is after
        entries = edb.get_timeseries_db().find({"user_id" : self.testUUID})
        res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
        logging.info(f"Purging complete: {res} entries remaining")
        print(f"Purging complete: {res} entries remaining")
        
        # A single entry with key 'stats/pipeline_time' should be present as this test involves running the pipeline
        stat_pipeline_key = entries[0].get('metadata').get('key')
        print(f"stat_pipeline_key = {stat_pipeline_key}")
        self.assertEqual(stat_pipeline_key,'stats/pipeline_time')
        self.assertEqual(res, 1)

        print("pipelineState_entries after full export purge =")
        pipelineState_entries = edb.get_pipeline_state_db().find({"user_id":  self.testUUID})
        for entry in pipelineState_entries:
            print(entry)

        # Run the restore pipeline
        logging.info(f"About to restore entries")
        print(f"About to restore entries")
        epr.run_restore_pipeline_for_user(self.testUUID, file_names)

        '''
        Test 4 - Verify that restoring timeseries data works with sample real data
        '''
        # Check how much data there is after
        res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
        res_stats_count = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID, "metadata.key" : 'stats/pipeline_time'})
        logging.info(f"Restoring complete: {res-1} entries restored")
        print(f"Restoring complete: {res-1} entries restored")

        # Two additional entries with key 'stats/pipeline_time' should be present - one from the purge pipeline, other from the restore pipeline
        self.assertEqual(res_stats_count, 2)
        self.assertEqual(res, 1908)

        # Test 5 - Verify that restoring timeseries data fails if data already exists
        # Duplicate key error is ignored hence no entries should be inserted
        logging.info("Attempting to load duplicate data...")
        print("Attempting to load duplicate data...")
        epr.run_restore_pipeline_for_user(self.testUUID, file_names)
        # Check how much data there is after
        res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
        logging.info(f"Restoring complete: {res-1} entries restored")
        print(f"Restoring complete: {res-1} entries restored")

        print("pipelineState_entries after running restore again =")
        pipelineState_entries = edb.get_pipeline_state_db().find({"user_id":  self.testUUID})
        for entry in pipelineState_entries:
            print(entry)
        # self.assertEqual(stat_pipeline_key,'stats/pipeline_time')
        # self.assertEqual(res, 1908)

    def testPurgeRestorePipelineIncremental(self):
            '''
            Test 1 - Verify that purging timeseries data works with sample real data
            '''
            # Check how much data there was before
            res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
            logging.info(f"About to purge {res} entries")
            print(f"About to purge {res} entries")
            self.assertEqual(res, 1906)

            # Run the purge pipeline
            file_names = epp.run_purge_pipeline_for_user(self.testUUID, os.environ.get('DATA_DIR', 'emission/archived'), "incremental")
            print("Exported file names: %s" % file_names)

            '''
            Test 2 - Assert the file exists after the export process and checking contents
            '''
            exported_data = []
            for file_name in file_names:
                self.assertTrue(pl.Path(file_name + ".gz").is_file()) 
                with gzip.open(file_name + ".gz", 'r') as ef:
                    exported_data.extend(json.loads(ef.read().decode('utf-8')))
            self.assertEqual(len(exported_data), 1906)

            last_few_objectIds = ['55b08d3e7d65cb39ee976def', '55b08d3e7d65cb39ee976df0', '55b08d3e7d65cb39ee976df1', '55b08e907d65cb39ee976e06', '55b08e907d65cb39ee976e07']
            for entry in exported_data[-5:]:
                self.assertIn(entry.get('_id').get('$oid'), last_few_objectIds)

            '''
            Test 3 - Verify that purging timeseries data works with sample real data
            '''
            # Check how much data there is after
            entries = edb.get_timeseries_db().find({"user_id" : self.testUUID})
            res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
            logging.info(f"Purging complete: {res} entries remaining")
            print(f"Purging complete: {res} entries remaining")
            
            # A single entry with key 'stats/pipeline_time' should be present as this test involves running the pipeline
            stat_pipeline_key = entries[0].get('metadata').get('key')
            print(f"stat_pipeline_key = {stat_pipeline_key}")
            self.assertEqual(stat_pipeline_key,'stats/pipeline_time')
            self.assertEqual(res, 1)

            print("pipelineState_entries after incremental export purge =")
            pipelineState_entries = edb.get_pipeline_state_db().find({"user_id":  self.testUUID})
            for entry in pipelineState_entries:
                print(entry)

            # Run the restore pipeline
            logging.info(f"About to restore entries")
            print(f"About to restore entries")
            print("File names: %s" % file_names)
            epr.run_restore_pipeline_for_user(self.testUUID, file_names)

            '''
            Test 4 - Verify that restoring timeseries data works with sample real data
            '''
            # Check how much data there is after
            res = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID})
            res_stats_count = edb.get_timeseries_db().count_documents({"user_id" : self.testUUID, "metadata.key" : 'stats/pipeline_time'})
            logging.info(f"Restoring complete: {res-1} entries restored")
            print(f"Restoring complete: {res-1} entries restored")

            # Two additional entries with key 'stats/pipeline_time' should be present - one from the purge pipeline, other from the restore pipeline
            self.assertEqual(res_stats_count, 2)
            self.assertEqual(res, 1908)

if __name__ == '__main__':
     etc.configLogging()
     unittest.main()
