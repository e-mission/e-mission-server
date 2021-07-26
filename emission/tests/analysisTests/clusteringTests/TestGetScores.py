import emission.analysis.modelling.tour_model.data_preprocessing as preprocess
import emission.analysis.modelling.tour_model.similarity as similarity
import emission.analysis.modelling.tour_model.get_scores as gs
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import json
import bson.json_util as bju
import emission.tests.common as etc

class TestGetScores(unittest.TestCase):
    def setUp(self):
        self.readAndStoreTripsFromFile("emission/tests/data/real_examples/fake_trips")
        self.user = self.testUUID
        self.radius = 100
        self.trips = preprocess.read_data(self.user)
        self.filter_trips = preprocess.filter_data(self.trips,self.radius)
        self.sim = similarity.similarity(self.filter_trips, self.radius)
        self.sim.bin_data()

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

    def test_compare_trip_orders(self):
        # this function contains pandas.testing.assert_frame_equal
        # if the orders of bin_trips and self.filter_trips(according to bins) are the same, the test will pass
        self.sim.delete_bins()
        self.bins = self.sim.bins
        self.bin_trips = self.sim.newdata
        gs.compare_trip_orders(self.bins, self.bin_trips, self.filter_trips)

    def test_score(self):
        labels_pred = []
        # we use all bins for testing
        for b in range(len(self.sim.bins)):
            for trip in self.sim.bins[b]:
                labels_pred.append(b)
        # labels_true = [0, 1, 2, 2, 3, 3, 3, 4]
        # labels_pred = [0, 0, 0, 0, 0, 1, 2, 3]
        homo_score = gs.score(self.filter_trips, labels_pred)
        self.assertEqual(homo_score,0.443)

    def test_get_score(self):
        homo_second = 0.443
        percentage_second = 0.5
        curr_score = gs.get_score(homo_second, percentage_second)
        self.assertEqual(curr_score,0.472)

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
