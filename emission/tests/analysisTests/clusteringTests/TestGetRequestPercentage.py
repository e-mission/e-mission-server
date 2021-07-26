from future import standard_library
standard_library.install_aliases()
import unittest
import emission.analysis.modelling.tour_model.similarity as similarity
import emission.analysis.modelling.tour_model.data_preprocessing as preprocess
import emission.analysis.modelling.tour_model.get_request_percentage as eamtg
import emission.analysis.modelling.tour_model.evaluation_pipeline as ep
import emission.tests.common as etc
import json
import bson.json_util as bju



class TestGetRequestPercentage(unittest.TestCase):
    def setUp(self):
        self.readAndStoreTripsFromFile("emission/tests/data/real_examples/fake_trips")
        self.user = self.testUUID
        self.radius = 100
        self.trips = preprocess.read_data(self.user)
        self.filter_trips = preprocess.filter_data(self.trips,self.radius)


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

    def test_match_day(self):
        sim = similarity.similarity(self.filter_trips, self.radius)
        sim.bin_data()
        sel_bin = sim.bins[0]
        # case 1: not same day trip
        trip = self.filter_trips[sel_bin[1]]
        self.assertEqual(eamtg.match_day(trip, sel_bin, self.filter_trips), False)
        # case 2: same day trip
        sel_bin = sim.bins[0]
        trip = self.filter_trips[sim.bins[1][0]]
        self.assertEqual(eamtg.match_day(trip, sel_bin, self.filter_trips), True)

    def test_match_month(self):
        sim = similarity.similarity(self.filter_trips, self.radius)
        sim.bin_data()
        sel_bin = sim.bins[0]
        # case 1: not same month trip
        trip = self.filter_trips[sel_bin[4]]
        self.assertEqual(eamtg.match_day(trip, sel_bin, self.filter_trips), False)
        # case 2: same month trip
        sel_bin = sim.bins[0]
        trip = self.filter_trips[sim.bins[1][0]]
        self.assertEqual(eamtg.match_day(trip, sel_bin, self.filter_trips), True)

    def test_bin_date(self):
        trip_ls = [0,1,2,3,4,5,6,7]
        self.assertEqual(eamtg.bin_date(trip_ls, self.filter_trips, day=True),[[0, 1, 2, 7], [3], [4], [5], [6]])
        self.assertEqual(eamtg.bin_date(trip_ls, self.filter_trips, month=True),[[0, 1, 2, 3, 4, 7], [5], [6]])

    def test_find_first_trip(self):
        test_bin = [0,1,2,3,4,5,6,7]
        self.assertEqual(eamtg.find_first_trip(self.filter_trips, test_bin),3)

    def test_requested_trips_ab_cutoff(self):
        bins = [[2,3,4,5,6]]
        # should request [3]
        request_trip_idx, no_request_idx = eamtg.requested_trips_ab_cutoff(bins, self.filter_trips)
        self.assertEqual((request_trip_idx, no_request_idx),([3], [2, 4, 5, 6]))

    def test_requested_trips_bl_cutoff(self):
        sim = similarity.similarity(self.filter_trips, self.radius)
        sim.bin_data()
        sim.delete_bins()
        request_idx_bl_cutoff = eamtg.requested_trips_bl_cutoff(sim)
        self.assertEqual(request_idx_bl_cutoff,[7, 1, 0])

    def test_get_requested_trips(self):
        sim = similarity.similarity(self.filter_trips, self.radius)
        sim.bin_data()
        sim.delete_bins()
        bins = sim.bins
        self.assertEqual(eamtg.get_requested_trips(bins, self.filter_trips, sim),[3, 7, 1, 0])

    def test_get_req_pct(self):
        sim = similarity.similarity(self.filter_trips, self.radius)
        sim.bin_data()
        sim.delete_bins()
        bins = sim.bins
        bin_trips = sim.newdata
        first_labels, track = ep.get_first_label_and_track(bins,bin_trips,self.filter_trips)
        new_labels = first_labels.copy()
        pct = eamtg.get_req_pct(new_labels, track, self.filter_trips, sim)
        self.assertEqual(pct,0.5)


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()

