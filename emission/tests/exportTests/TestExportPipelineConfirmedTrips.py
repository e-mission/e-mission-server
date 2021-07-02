from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import json
import bson.json_util as bju
import emission.storage.timeseries.abstract_timeseries as esta
import gzip
import emission.tests.common as etc
import emission.pipeline.export_stage as epe

class TestDataReadingSamples(unittest.TestCase):
    def readDataFromFile(self, dataFile):
        with open(dataFile) as dect:
            raw_data = json.load(dect, object_hook = bju.object_hook)
            return raw_data

    def testReadDataFromFile(self):
        raw_data = self.readDataFromFile("emission/tests/data/real_examples/shankari_2015-07-22")
        self.printData(raw_data) 

    def printData(self, data):
        for t in data:
            if t['metadata']['key'] == "analysis/confirmed_trip":
                print(f"{t['metadata']['key']}: {t['data']['start_fmt_time']} -> {t['data']['end_fmt_time']}")

    def readDataFromExportFile(self, dataFile):
        with gzip.open(dataFile, 'r') as ef:
            exported_data = json.loads(ef.read().decode('utf-8'))
        return exported_data
 
    def testReadDataFromExportFile(self):
        exportData = self.readDataFromExportFile("archive_5d5fc80b-c031-4e43-8d64-52fb29aefc94_None_1625254869.648768.gz")
        self.printData(exportData)


if __name__ == '__main__':
     etc.configLogging()
     unittest.main()
