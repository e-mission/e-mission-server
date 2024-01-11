from future import standard_library
standard_library.install_aliases()
from builtins import *
import os
import json
import logging
import tempfile
import unittest
from pymongo import errors

import emission.tests.common as etc
import emission.core.get_database as edb
import emission.storage.pipeline_queries as esp
import emission.core.wrapper.pipelinestate as ecwp
from bin.purge_user_timeseries import purgeUserTimeseries
from bin.restore_user_timeseries import restoreUserTimeseries


class TestPurgeRestoreUserTimeseries(unittest.TestCase):
    def setUp(self):
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")
        etc.runIntakePipeline(self.testUUID)

    def tearDown(self):
        etc.dropAllCollections(edb._get_current_db())

    def testPurgeRestoreUserTimeseries(self):
        with tempfile.TemporaryDirectory(dir="/var/tmp") as tmpdirname:
            cstate = esp.get_current_state(self.testUUID, ecwp.PipelineStages.CREATE_CONFIRMED_OBJECTS)
            last_ts_run = cstate['last_ts_run']
            self.assertTrue(last_ts_run > 0)
            
            '''
            Test 1 - Verify that purging timeseries data works with sample real data
            '''
            # Check how much data there was before
            res = edb.get_timeseries_db().count_documents({"user_id": self.testUUID, "metadata.write_ts": { "$lt": last_ts_run}})
            logging.info(f"About to purge {res} entries")
            self.assertEqual(res, 1906)

            # Run the purge function
            file_prefix = "some_fancy_prefix_"    
            exportFileFlags = {
                'json_export': True,
                'csv_export': True
            }
            filename = tmpdirname + "/" + file_prefix + str(self.testUUID)
            purgeUserTimeseries(exportFileFlags, str(self.testUUID), dir_name=tmpdirname, file_prefix=file_prefix)

            # Check how much data there is after
            res = edb.get_timeseries_db().count_documents({"user_id": self.testUUID, "metadata.write_ts": { "$lt": last_ts_run}})
            logging.debug(f"Purging complete: {res} entries remaining")
            self.assertEqual(res, 0)

            # Check that data was properly saved (1906 lines of data + 1 line of header)
            with open(filename + ".csv", 'r') as f:
                csv_lines = f.readlines()
                logging.debug(f"No. of entries in CSV file: {len(csv_lines)}")
                self.assertEqual(len(csv_lines), 1907)

            '''
            Test 2 - Verify that restoring timeseries data works with sample real data
            '''
            # Run the restore function
            logging.info(f"About to restore entries")
            restoreUserTimeseries(filename + ".json")

            # Check how much data there is after
            res = edb.get_timeseries_db().count_documents({"user_id": self.testUUID, "metadata.write_ts": { "$lt": last_ts_run}})
            logging.info(f"Restoring complete: {res} entries restored")
            self.assertEqual(res, 1906)

            '''
            Test 3 - Verify that restoring timeseries data fails if duplicate data loaded
            '''
            logging.info("Attempting to load duplicate data...")
            with self.assertRaises(errors.BulkWriteError) as bwe:
                restoreUserTimeseries(filename + ".json")
            write_errors = bwe.exception.details.get('writeErrors', [])
            for write_error in write_errors:
                index = write_error.get('index', None)
                code = write_error.get('code', None)
                error_message = write_error.get('errmsg', None)
                logging.info(f"Error at index {index}: {error_message}")

        '''
        Test 4 - Restoring from an empty JSON file should fail as the insert command needs non-empty document data
        '''
        emptyJsonFile = '/Users/mmahadik/Documents/GitHub/logs/data/restore_csv/empty_file.json'
        with open(emptyJsonFile, 'w') as file:
            json.dump([], file)
        logging.info("Attempting to load empty data...")
        with self.assertRaises(TypeError) as te:
            restoreUserTimeseries(emptyJsonFile)
        self.assertEqual(str(te.exception), "documents must be a non-empty list")
        os.remove(emptyJsonFile)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    etc.configLogging()
    unittest.main()