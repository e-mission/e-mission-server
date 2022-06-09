import unittest
import logging
import pymongo
import json
import bson.json_util as bju
import pandas as pd
import numpy as np
import bson.objectid as boi
from uuid import UUID

import emission.tests.common as etc

import emission.analysis.modelling.tour_model.similarity as eamts

# Class to test auxiliary functions (such as returning the result labels)
# in similarity.
# TODO: This should be merged into the full similarity tests once they are complete

class TestSimilarityAux(unittest.TestCase):
    def setUp(self):
        # The _id objects don't need to be object ids
        # but having them be object ids does not hurt,
        # makes it easier to create dataframes, and is
        # a little bit more true to life, so we can catch
        # regressions in the pandas code wrt matching by objects, for example
        N_OBJECTS = 20
        self.all_trips = [{"_id": boi.ObjectId()} for i in range(N_OBJECTS)]
        # logging.debug(self.all_trips)
        self.too_short_indices = list(range(0,N_OBJECTS,5))
        logging.debug("too_short_indices %s" % self.too_short_indices)

        self.filtered_trips = [t for i, t in enumerate(self.all_trips) if i not in self.too_short_indices]

        self.all_bins = np.array(range(0,N_OBJECTS)).reshape(4,5)
        logging.debug("bins are %s" % self.all_bins)

        self.after_filter_bins = np.array(range(0,16)).reshape(4,4)
        self.after_filter_bins_all_trips_index = [[t for t in ob if t not in self.too_short_indices] for ob in self.all_bins]
        logging.debug(f"before cutoff {self.after_filter_bins}")

        IGNORED = 0
        self.curr_sim = eamts.similarity(self.all_trips, IGNORED)

        
    def tearDown(self):
        pass

    def testEmptyDataFrame(self):
        self.curr_sim = eamts.similarity([], 0)
        self.assertEqual(self.curr_sim.get_result_labels().to_list(), [])

    def testEmptyTooShortDataFrame(self):
        self.curr_sim = eamts.similarity([{"_id": boi.ObjectId()}], 0)
        self.curr_sim.filtered_data = []
        self.assertEqual(self.curr_sim.get_result_labels().to_list(), [-1])

    def testBeforeFiltering(self):
        # before filtering, everything should be noise
        result = self.curr_sim.get_result_labels()
        # logging.debug(result)
        self.assertEqual(result.to_list(), [-1] * len(self.all_trips))

    def testBeforeFilteringAfterBinning(self):
        # before filtering, but after binning
        self.curr_sim.bins = self.all_bins
        exp_result = [0] * 5 + [1] * 5 + [2] * 5 + [3] * 5
        self.assertEqual(self.curr_sim.get_result_labels().to_list(), exp_result)

    def testAfterFilteringBeforeBinning(self):
        # after filtering, but before binning
        self.curr_sim.filtered_data = self.filtered_trips
        exp_result = pd.Series([-1] * 20)
        exp_result.loc[self.too_short_indices] = -2
        # logging.debug(exp_result)
        self.assertEqual(self.curr_sim.get_result_labels().to_list(), exp_result.to_list())

    def testAfterFilteringAfterBinningBeforeCutoff(self):
        # after filtering, after binning
        self.curr_sim.filtered_data = self.filtered_trips
        self.curr_sim.bins = self.after_filter_bins
        exp_result = pd.Series([-1] * 20)
        for i, b in enumerate(self.after_filter_bins_all_trips_index):
            exp_result.loc[b] = i
        exp_result.loc[self.too_short_indices] = -2
        self.assertEqual(self.curr_sim.get_result_labels().to_list(), exp_result.to_list())
    
    def testBeforeFilteringAfterBinningAfterCutoff(self):
        # drop the last two bins to simulate a cutoff
        self.curr_sim.bins = self.all_bins[:-2]
        exp_result = pd.Series([-1] * 20)
        for i, b in enumerate([list(range(5)), list(range(5,10))]):
            exp_result.loc[b] = i
        self.assertEqual(self.curr_sim.get_result_labels().to_list(), exp_result.to_list())
        
    def testAfterFilteringAfterBinningAfterCutoff(self):
        # drop the last two bins to simulate a cutoff
        self.curr_sim.filtered_data = self.filtered_trips
        self.curr_sim.bins = self.after_filter_bins[:-2]
        exp_result = pd.Series([-1] * 20)
        for i, b in enumerate(self.after_filter_bins_all_trips_index[:-2]):
            exp_result.loc[b] = i
        exp_result.loc[self.too_short_indices] = -2
        self.assertEqual(self.curr_sim.get_result_labels().to_list(), exp_result.to_list())

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
