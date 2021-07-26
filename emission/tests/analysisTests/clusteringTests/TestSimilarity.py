import emission.analysis.modelling.tour_model.data_preprocessing as preprocess
import emission.analysis.modelling.tour_model.similarity as similarity
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import json
import bson.json_util as bju
import emission.tests.common as etc

# This test file is to test the functions that are used in the
class TestSimilarity(unittest.TestCase):
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

    def test_within_radius(self):
        # case 1: start and end location are within radius
        in_range = similarity.within_radius(-122.40998884982407, 37.809339507025655,
                                            -122.4101610462097, 37.80925081700211, self.radius)
        self.assertEqual(in_range,True)
        # case 2: start and end location are not within radius
        in_range = similarity.within_radius(-122.40998884982407, 37.809339507025655,
                                            -122.41296471977945, 37.8079948386731, self.radius)
        self.assertEqual(in_range,False)


    def test_filter_too_short(self):
        all_trips = preprocess.read_data(self.user)
        valid_trips = similarity.filter_too_short(all_trips, self.radius)
        self.assertEqual(len(valid_trips),10)

    def test_bin_data(self):
        trips = preprocess.read_data(self.user)
        sim = similarity.similarity(trips, self.radius)
        filter_trips = sim.data
        sim.bin_data()
        self.assertEqual(sim.bins,[[4, 5, 6, 7, 8], [0], [1], [2], [3], [9]])

    def test_delete_bins(self):
        trips = preprocess.read_data(self.user)
        sim = similarity.similarity(trips, self.radius)
        filter_trips = sim.data
        sim.bin_data()
        sim.delete_bins()
        bins = sim.bins
        bin_trips = sim.newdata
        self.assertEqual(bins,[[4, 5, 6, 7, 8]])
        self.assertEqual(len(bin_trips),5)


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()


