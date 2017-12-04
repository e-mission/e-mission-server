from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from past.builtins import cmp
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

import unittest
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.cluster_pipeline as cp
import uuid as uu
import emission.tests.common as etc

import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.storage.timeseries.format_hacks.move_filter_field as estfm
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.clean_and_resample as eaicr

class ClusterPipelineTests(unittest.TestCase):
    def setUp(self):
        self.clearRelevantSections()
        self.import_test_info()
        self.RADIUS = 200

    def clearRelevantSections(self):
        edb.get_analysis_timeseries_db().drop()

    def import_test_info(self):
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-27")
        eaicf.filter_accuracy(self.testUUID)
        etc.runIntakePipeline(self.testUUID)

    def testSanity(self):
                cp.main(self.testUUID)

    def testReadData(self): 
        data = cp.read_data(uuid=self.testUUID)
        
        # Test to make sure something is happening
        self.assertTrue(len(data) > 5)

        # Test to make sure that the trips are mapped to the correct uuid
        bad_data = cp.read_data(uuid="FakeUUID")
        self.assertEqual(len(bad_data), 0)

    def testRemoveNoise(self):
        data = cp.read_data(uuid=self.testUUID)

        # Test to make sure the code doesn't break on an empty dataset
        new_data, bins = cp.remove_noise(None, self.RADIUS)
        self.assertTrue(len(new_data) == len(bins) == 0)        
        
        #Test to make sure some or no data was filtered out, but that nothing was added after filtering
        new_data, bins = cp.remove_noise(None, self.RADIUS)
        self.assertTrue(len(new_data) <= len(data))
        
        # Make sure there are not more bins than data; that wouldnt make sense
        self.assertTrue(len(bins) <= len(data))

    def testCluster(self):
        data = cp.read_data(uuid=self.testUUID)

        # Test to make sure empty dataset doesn't crash the program
        clusters, labels, new_data = cp.cluster([], 10)
        self.assertTrue(len(new_data) == clusters == len(labels) == 0)

        # Test to make sure clustering with noise works
        clusters, labels, new_data = cp.cluster(data, 10)
        self.assertEqual(len(labels), len(new_data))
        self.assertEqual(cmp(new_data, data), 0)
        
        # Test to make sure clustering without noise works
        data, bins = cp.remove_noise(data, self.RADIUS)
        clusters, labels, new_data = cp.cluster(data, len(bins))
        self.assertTrue(clusters == 0 or len(bins) <= clusters <= len(bins) + 10)
        
    def testClusterToTourModel(self):
        # Test to make sure it doesn't crash on a empty dataset
        data = cp.cluster_to_tour_model(None, None)
        self.assertFalse(data)
        
        # Test with the real dataset
        data = cp.read_data(uuid=self.testUUID)
        data, bins = cp.remove_noise(data, self.RADIUS)
        n, labels, data = cp.cluster(data, len(bins))
        tour_dict = cp.main(uuid=self.testUUID)
        self.assertTrue(len(tour_dict) <= n)


if __name__ == "__main__":
    unittest.main()
