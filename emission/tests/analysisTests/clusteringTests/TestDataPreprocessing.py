import emission.analysis.modelling.tour_model.data_preprocessing as preprocess
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import json
import bson.json_util as bju
import emission.tests.common as etc


class TestDataPreprocessing(unittest.TestCase):
    def setUp(self):
        self.readAndStoreTripsFromFile("emission/tests/data/real_examples/fake_trips")
        self.user = self.testUUID
        self.radius = 100

    def tearDown(self):
        self.clearDBEntries()

    def readAndStoreTripsFromFile(self, dataFile):
        import emission.core.get_database as edb
        atsdb = edb.get_analysis_timeseries_db()
        etc.createAndFillUUID(self)
        with open(dataFile) as dect:
            expected_confirmed_trips = json.load(dect, object_hook=bju.object_hook)
            for t in expected_confirmed_trips:
                t["user_id"] = self.testUUID
                edb.save(atsdb, t)

    def clearDBEntries(self):
        import emission.core.get_database as edb
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID})

    def test_read_data(self):
        trips = preprocess.read_data(self.user)
        self.assertEqual(len(trips), 10)

    def test_filter_data(self):
        trips = preprocess.read_data(self.user)
        filter_trips = preprocess.filter_data(trips,self.radius)
        self.assertEqual(len(filter_trips), 8)

    def test_extract_features(self):
        trips = preprocess.read_data(self.user)
        filter_trips = preprocess.filter_data(trips,self.radius)
        X = preprocess.extract_features(filter_trips)
        self.assertEqual(len(X), 8)
        self.assertEqual(X[0], [-122.0857861, 37.3898049, -122.0826931,
                                37.3914184, 1047.1630675866315, 792.4609999656677])

    def test_split_data(self):
        trips = preprocess.read_data(self.user)
        filter_trips = preprocess.filter_data(trips,self.radius)
        train_idx, test_idx = preprocess.split_data(filter_trips)
        self.assertEqual(len(train_idx),5)
        self.assertEqual(len(test_idx), 5)
        self.assertGreaterEqual(len(train_idx[0]),len(test_idx[0]),'the number of trips in train_idx should be greater '
                                                                   'than the one in test_idx')

    def test_get_subdata(self):
        trips = preprocess.read_data(self.user)
        filter_trips = preprocess.filter_data(trips,self.radius)
        train_set_idx = [[0,1,2,3,4],[0,1,2,4,5]]
        collect_sub_data = preprocess.get_subdata(filter_trips, train_set_idx)
        compare_idx = filter_trips.index(collect_sub_data[0][4])
        self.assertEqual(compare_idx, 4)


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()

