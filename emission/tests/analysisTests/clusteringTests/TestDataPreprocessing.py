import emission.core.wrapper.localdate as ecwl
import emission.analysis.modelling.tour_model.data_preprocessing as preprocess

from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import json
import bson.json_util as bju
import emission.storage.timeseries.abstract_timeseries as esta

import emission.tests.common as etc


class TestDataPreprocessing(unittest.TestCase):

    # should setup user = [self.testUUID], radius = 100
    # do we need teardown if we don't use databse?


    def test_read_data(self):
        dataFile = "emission/tests/data/real_examples/shankari_2016-06-20"
        ld = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 20})
        with open(dataFile+".ground_truth") as gfp:
            ground_truth = json.load(gfp, object_hook=bju.object_hook)

        etc.setupRealExample(self, dataFile)
        # if (not preload):
        self.entries = json.load(open(dataFile+".user_inputs"), object_hook = bju.object_hook)
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        confirmed_trips = list(ts.find_entries(["analysis/confirmed_trip"], None))
        with open(dataFile+".expected_confirmed_trips") as dect:
            expected_confirmed_trips = json.load(dect, object_hook = bju.object_hook)
        print('confirmed_trips',confirmed_trips)
        user = [self.testUUID]
        trips = preprocess.read_data(user)
        print('trips ', trips)
        # I don't know how to assertEqual here


    def test_filter_data(self):
        radius = 100
        # - trips: should be read from a file or from database
        user = [self.testUUID]
        trips = preprocess.read_data(user)
        filter_trips = preprocess.filter_data(trips,radius)
        # assertEqual

    def test_extract_features(self):
        user = [self.testUUID]
        radius = 100
        trips = preprocess.read_data(user)
        filter_trips = preprocess.filter_data(trips,radius)
        X = preprocess.extract_features(filter_trips)
        # assertEqual

    def test_split_data(self):
        user = [self.testUUID]
        radius = 100
        trips = preprocess.read_data(user)
        filter_trips = preprocess.filter_data(trips,radius)
        train_idx, test_idx = preprocess.split_data(filter_trips)
        # assertEqual

    def test_get_subdata(self):
        user = [self.testUUID]
        radius = 100
        trips = preprocess.read_data(user)
        filter_trips = preprocess.filter_data(trips,radius)
        train_set_idx = [0,1,2,3,4]
        collect_sub_data = preprocess.get_subdata(filter_trips, train_set_idx)
        # assertEqual


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()

