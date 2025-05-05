from future import standard_library
standard_library.install_aliases()
from builtins import *
import os
import tempfile
import unittest
import emission.tests.common as etc
import emission.core.get_database as edb
import emission.storage.pipeline_queries as esp
import emission.core.wrapper.pipelinestate as ecwp
from bin.purge_user_timeseries import purgeUserTimeseries


class TestPurgeUserTimeseries(unittest.TestCase):
    def setUp(self):
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")
        etc.runIntakePipeline(self.testUUID)

    def testPurgeUserTimeseries(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as tmpdirname:
            cstate = esp.get_current_state(self.testUUID, ecwp.PipelineStages.CREATE_CONFIRMED_OBJECTS)
            last_ts_run = cstate['last_ts_run']
            self.assertTrue(last_ts_run > 0)
            
            # Check how much data there was before
            res = edb.get_timeseries_db().count_documents({"user_id": self.testUUID, "metadata.write_ts": { "$lt": last_ts_run}})
            self.assertEqual(res, 1906)

            # Run the purge function
            file_prefix = "some_fancy_prefix_"
            purgeUserTimeseries(str(self.testUUID), dir_name=tmpdirname, file_prefix=file_prefix)

            # Check how much data there is after
            res = edb.get_timeseries_db().count_documents({"user_id": self.testUUID, "metadata.write_ts": { "$lt": last_ts_run}})
            self.assertEqual(res, 0)

            # Check that data was properly saved (1906 lines of data + 1 line of header)
            with open(tmpdirname + "/" + file_prefix + str(self.testUUID) + ".csv", 'r') as f:
                self.assertTrue(f.readlines(), 1907)
    
    def tearDown(self):
        etc.dropAllCollections(edb._get_current_db())


if __name__ == '__main__':
     etc.configLogging()
     unittest.main()
