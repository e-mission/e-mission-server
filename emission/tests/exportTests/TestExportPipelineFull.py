from future import standard_library
standard_library.install_aliases()
from builtins import *
import os
from os import path
import emission.tests.common as etc
import emission.exportdata.export_data as eeed
import unittest
import tempfile

class TestExportPipelinePipelineFull(unittest.TestCase):
    def testExportPipelineFull(self):
        #Setup
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")
        etc.runIntakePipeline(self.testUUID)

        #Create a temporary directory within the emission folder
        with tempfile.TemporaryDirectory(dir='/tmp') as tmpdirname:
            self.assertTrue(path.isdir(tmpdirname))

            #Set the envrionment variable
            os.environ['DATA_DIR'] = tmpdirname
            self.assertEqual(os.environ['DATA_DIR'], tmpdirname)

            #Run the export pipeline
            eeed.export_data(self.testUUID)
            directory = os.listdir(tmpdirname)

            #Check to see if there is a file in the temp directory
            self.assertTrue(len(directory) == 1)

            #Check to make sure the file is of the correct UUID
            uuid = str(self.testUUID)
            file_name = directory[0]
            self.assertTrue(uuid in file_name)

if __name__ == '__main__':
     etc.configLogging()
     unittest.main()
