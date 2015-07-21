# Standard imports
import unittest
from datetime import datetime
import logging
import json
import numpy as np

# Our imports
from emission.core.get_database import get_section_db
import emission.analysis.classification.cleaning.gps_smoothing as tags

class GpsSmoothingTests(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testSmoothBoundaryOneZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/caltrain_one_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_boundary(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 1)
        self.assertEqual(removed_indices, [68])

    def testSmoothDeviationOneZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/caltrain_one_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_deviation(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 5)
        self.assertEqual(removed_indices, [37, 38, 45, 46, 68])

    def testSmoothPosdapOneZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/caltrain_one_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_posdap(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 1)
        self.assertEqual(removed_indices, [68])

    def testSmoothMaxDeviationOneZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/caltrain_one_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_max_boundary(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 1)
        self.assertEqual(removed_indices, [68])

    def testSmoothMaxPosdapOneZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/caltrain_one_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_max_posdap(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 1)
        self.assertEqual(removed_indices, [68])

    def testSmoothZigZagBoundaryOneZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/caltrain_one_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_zigzag_boundary(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 2)
        self.assertEqual(removed_indices, [68, 69])

    def testSmoothZigZagPosdapOneZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/caltrain_one_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_zigzag_posdap(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 2)
        self.assertEqual(removed_indices, [68, 69])

    def testSmoothBoundaryMultiZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/caltrain_multi_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_boundary(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 15)
        self.assertEqual(removed_indices, [21, 26, 28, 29, 30, 40, 41, 42, 43, 46, 47, 48, 49, 50, 57])

    def testSmoothDeviationMultiZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/caltrain_multi_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_deviation(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 3)
        self.assertEqual(removed_indices, [21, 22, 57])

    def testSmoothPosdapMultiZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/caltrain_multi_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_posdap(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 10)
        self.assertEqual(removed_indices, [21, 26, 27, 40, 41, 42, 43, 44, 45, 57])

    def testSmoothMaxBoundaryMultiZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/caltrain_multi_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_max_boundary(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 2)
        self.assertEqual(removed_indices, [46, 58])

    def testSmoothMaxPosdapMultiZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/caltrain_multi_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_max_posdap(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 1)
        self.assertEqual(removed_indices, [46])

    def testSmoothZigZagBoundaryMultiZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/caltrain_multi_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_zigzag_boundary(tp)
        logging.info("removed indices = %s" % removed_indices)
        # self.assertEqual(len(removed_indices), 23)
        self.assertEqual(removed_indices, [26, 27, 31, 32, 33, 34, 35, 36, 37, 38, 39, 44, 45, 57, 58, 60, 64, 65, 66, 67, 68])

    def testSmoothZigZagPosdapMultiZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/caltrain_multi_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_zigzag_posdap(tp)
        logging.info("removed indices = %s" % removed_indices)
        # self.assertEqual(len(removed_indices), 2)
        self.assertEqual(removed_indices, [26, 27, 31, 32, 33, 34, 35, 36, 37, 38, 39, 44, 45, 57, 58, 60, 64, 65, 66, 67, 68])

    def testSmoothBoundaryWalkOneZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/walk_one_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_boundary(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 0)
        self.assertEqual(removed_indices, [])

    def testSmoothDeviationWalkOneZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/walk_one_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_deviation(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 4)
        self.assertEqual(removed_indices, [5, 6, 7, 8])

    def testSmoothMaxDeviationWalkOneZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/walk_one_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_max_boundary(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 1)
        self.assertEqual(removed_indices, [5])

    def testSmoothMaxPosdapWalkOneZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/walk_one_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_max_posdap(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 0)
        self.assertEqual(removed_indices, [])

    def testSmoothZigZagBoundaryWalkOneZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/walk_one_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_zigzag_boundary(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 1)
        self.assertEqual(removed_indices, [5])

    def testSmoothZigZagPosdapWalkOneZigZag(self):
        tp = json.load(open("emission/tests/data/smoothing_data/walk_one_zigzag.json"))
        (retained_indices, removed_indices) = tags.smooth_zigzag_boundary(tp)
        logging.info("removed indices = %s" % removed_indices)
        self.assertEqual(len(removed_indices), 1)
        self.assertEqual(removed_indices, [5])

    def testRemoveNonConsecutive(self):
        indexList = [50, 51, 52, 53, 54, 55, 56, 58, 60, 64, 65, 66, 67, 68]
        removed_points = []
        tags.remove_non_consecutive(indexList, removed_points)
        self.assertEqual(removed_points, [58, 60, 64, 65, 66, 67, 68])

        indexList = [13, 14, 15, 16, 17, 18, 19, 20, 26, 27, 31, 32, 33, 34, 35, 36, 37, 38, 39, 44, 45]
        removed_points = []
        tags.remove_non_consecutive(indexList, removed_points)
        self.assertEqual(removed_points, [26, 27, 31, 32, 33, 34, 35, 36, 37, 38, 39, 44, 45])

    def testStripZigzagPoints(self):
        tp = json.load(open("emission/tests/data/smoothing_data/caltrain_multi_zigzag.json"))
        removed_points = tags.strip_zigzag_points(tp)
        self.assertEqual(removed_points, [26, 27, 31, 32, 33, 34, 35, 36, 37, 38, 39, 44, 45, 58, 60, 64, 65, 66, 67, 68])

    def testDeletePoints(self):
        tp = json.load(open("emission/tests/data/smoothing_data/walk_one_zigzag.json"))
        new_tp = tags.delete_points(tp, [4, 5, 6, 10, 11, 12])
        self.assertEqual(len(new_tp), 9)
        self.assertEqual(new_tp[5]["track_location"]["coordinates"], [-122.0861031, 37.391046])
        self.assertEqual(new_tp[8]["track_location"]["coordinates"], [-122.0862501, 37.3909839])

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
