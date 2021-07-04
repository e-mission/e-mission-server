from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import json
import bson.json_util as bju
import emission.storage.timeseries.abstract_timeseries as esta
import gzip
import glob
import emission.tests.common as etc
import emission.pipeline.export_stage as epe
import emission.storage.pipeline_queries as espq
import emission.exportdata.export_data as eeed

class TestExportPipelineConfirmedTripsandBackground(unittest.TestCase):
    def testExportPipeline(self):
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")
        etc.runIntakePipeline(self.testUUID)
        eeed.export_data(self.testUUID)
        
        #Data has been run through both pipelines, file has been exported.
        tq = espq.get_time_range_for_export_data(self.testUUID)
        exported_file_name = "emission/archived/archive_%s_%s_%s.gz" % (self.testUUID, tq.startTs, tq.endTs)
        
        #trim this file name from the back 
        #generate a list of filesnames that are close (exports that are recent with this user, small list)
	#this is a HACK, is there a better way? I am lost!(hoping list is 1)
        
        exported_file_name_trimmed = exported_file_name[:len(exported_file_name) - 15]
        file_list = []
        for name in glob.glob("%s*.gz" % exported_file_name_trimmed):
            file_list.append(name)
        for exported_file_name in file_list:
            with gzip.open(exported_file_name, 'r') as ef:
                exported_data = json.loads(ef.read().decode('utf-8'))
            
            #Testing to confirm we have confirmed trips in the exported data, none in raw, should not be equal

            confirmed_trips_exported = []
            for t in exported_data:
                if t['metadata']['key'] == "analysis/confirmed_trip":
                    confirmed_trips_exported.append(t)
            raw_data = self.readDataFromFile("emission/tests/data/real_examples/shankari_2015-07-22")
            confirmed_trips_raw = []
            for t in raw_data:
                if t['metadata']['key'] == "analysis/confirmed_trip":
                    confirmed_trips_raw.append(t)
            self.assertNotEqual(confirmed_trips_exported, confirmed_trips_raw)

            #Testing to confirm there is no confirmed trips in the raw data (validates prev test)

            empty_list = []
            self.assertEqual(empty_list, confirmed_trips_raw)

            #Testing to confirm the length of data entries, should be the same for raw and processed

            background_location_exported = []
            for t in exported_data:
                if t['metadata']['key'] == "background/location":
                    background_location_exported.append(t)
            background_location_raw = []
            for t in raw_data:
                if t['metadata']['key'] == "background/location":
                    background_location_raw.append(t)
            self.assertEqual(len(background_location_exported), len(background_location_raw)) 
    
    def readDataFromFile(self, dataFile):
        with open(dataFile) as dect:
            raw_data = json.load(dect, object_hook = bju.object_hook)
            return raw_data


if __name__ == '__main__':
     etc.configLogging()
     unittest.main()
