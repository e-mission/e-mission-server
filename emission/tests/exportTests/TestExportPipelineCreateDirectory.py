from future import standard_library
standard_library.install_aliases()
from builtins import *
import os
from os import path
import emission.tests.common as etc
import emission.exportdata.export_data as eeed
import unittest
import tempfile

class TestExportPipelineCreateDirectory(unittest.TestCase):
    def testExportPipelineCreateDirectory(self):
        #Setup
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")
        etc.runIntakePipeline(self.testUUID)

        #Set the os.environ['DATA_DIR'] to a directory path that does not yet exist
        os.environ['DATA_DIR'] = '/tmp/nonexistent'
        self.assertTrue(os.environ['DATA_DIR'], '/tmp/nonexistent')

        #Run the export pipeline
        eeed.export_data(self.testUUID)
        directory = os.listdir(os.environ['DATA_DIR'])
        
        #Check to see if there is a file in the directory
        uuid = str(self.testUUID)
        file_name = directory[0]
        self.assertTrue(uuid in file_name)

        #Remove the file from the directory
        dir = os.environ['DATA_DIR']
        for f in os.listdir(dir):
            os.remove(os.path.join(dir, f))
        os.rmdir(dir)
        self.assertFalse(os.path.isdir(os.environ['DATA_DIR']))

if __name__ == '__main__':
     etc.configLogging()
     unittest.main()
