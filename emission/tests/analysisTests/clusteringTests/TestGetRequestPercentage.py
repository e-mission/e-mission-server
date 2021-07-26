from future import standard_library
standard_library.install_aliases()
import unittest
import emission.analysis.modelling.tour_model.similarity as similarity
import emission.analysis.modelling.tour_model.data_preprocessing as preprocess

import emission.analysis.modelling.tour_model.get_request_percentage as eamtg
import pandas as pd
import emission.tests.common as etc
import sklearn.cluster as sc
import numpy as np
import json
import bson.json_util as bju


class TestGetRequestPercentage(unittest.TestCase):
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

    def test_match_day(self):
        trips = preprocess.read_data(self.user)
        filter_trips = preprocess.filter_data(trips,self.radius)
        sim = similarity.similarity(filter_trips, radius)

    # def test_match_day(self):
    #     # case 1: bin contains indices & trip matches selected trip in filter_trips
    #     bin = [0,1,2]
    #     trip = {'data':{'start_local_dt':{'year':2020,'month':8,'day':14}}}
    #     filter_trips = [{'data':{'start_local_dt':{'year':2020,'month':8,'day':14}}}]
    #     self.assertEqual(eamtg.match_day(trip, bin, filter_trips), True)
    #     # case 2: bin = True & trip doesn't match selected trip in filter_trips
    #     filter_trips = [{'data': {'start_local_dt': {'year': 2020, 'month': 8, 'day': 18}}}]
    #     self.assertEqual(eamtg.match_day(trip, bin, filter_trips), False)
    #     #case 3: bin is none & trip matches selected trip in filter_trips
    #     bin = None
    #     filter_trips = [{'data': {'start_local_dt': {'year': 2020, 'month': 8, 'day': 14}}}]
    #     self.assertEqual(eamtg.match_day(trip, bin, filter_trips), False)
    #
    #
    # def test_match_month(self):
    #     # case 1: bin contains indices & trip matches selected trip in filter_trips
    #     bin = [0,1,2]
    #     trip = {'data':{'start_local_dt':{'year':2020,'month':8,'day':14}}}
    #     filter_trips = [{'data':{'start_local_dt':{'year':2020,'month':8,'day':14}}}]
    #     self.assertEqual(eamtg.match_month(trip, bin, filter_trips), True)
    #     # case 2: bin = True & trip doesn't match selected trip in filter_trips
    #     filter_trips = [{'data': {'start_local_dt': {'year': 2020, 'month': 7, 'day': 18}}}]
    #     self.assertEqual(eamtg.match_month(trip, bin, filter_trips), False)
    #     #case 3: bin is none & trip matches selected trip in filter_trips
    #     bin = None
    #     filter_trips = [{'data': {'start_local_dt': {'year': 2020, 'month': 8, 'day': 14}}}]
    #     self.assertEqual(eamtg.match_month(trip, bin, filter_trips), False)
    #
    #
    # def test_bin_date(self):
    #     # case 1: bin day
    #     trip_ls = [0,1,2]
    #     filter_trips1 = [{'data': {'start_local_dt': {'year': 2020, 'month': 8, 'day': 14}}},
    #                     {'data': {'start_local_dt': {'year': 2020, 'month': 8, 'day': 14}}},
    #                     {'data': {'start_local_dt': {'year': 2020, 'month': 7, 'day': 18}}}]
    #     self.assertEqual(eamtg.bin_date(trip_ls, filter_trips1, day=True), [[0,1],[2]])
    #     # case 2: bin month
    #     filter_trips2 = [{'data': {'start_local_dt': {'year': 2020, 'month': 8, 'day': 15}}},
    #                     {'data': {'start_local_dt': {'year': 2020, 'month': 8, 'day': 14}}},
    #                     {'data': {'start_local_dt': {'year': 2020, 'month': 7, 'day': 18}}}]
    #     self.assertEqual(eamtg.bin_date(trip_ls, filter_trips2, month=True), [[0,1],[2]])
    #
    #
    # def test_find_first_trip(self):
    #     import time
    #     time1 = "Thu Jan 28 22:24:24 2020"
    #     time2 = "Sat Jan 30 23:24:24 2020"
    #     time3 = "Sun Jan 31 20:24:24 2020"
    #     bin = [0,1,2]
    #     ts1 = time.mktime(time.strptime(time1, "%a %b %d %H:%M:%S %Y"))
    #     ts2 = time.mktime(time.strptime(time2, "%a %b %d %H:%M:%S %Y"))
    #     ts3 = time.mktime(time.strptime(time3, "%a %b %d %H:%M:%S %Y"))
    #     filter_trips = [{'data': {'start_ts': ts1}},
    #                     {'data': {'start_ts': ts2}},
    #                     {'data': {'start_ts': ts3}}]
    #
    #     self.assertEqual(eamtg.find_first_trip(filter_trips, bin),0)
    #
    #
    # def test_requested_trips_ab_cutoff(self):
    #     import time
    #     time1 = "Thu Jan 28 22:24:24 2020"
    #     time2 = "Sat Jan 30 23:24:24 2020"
    #     time3 = "Sun Jan 31 20:24:24 2020"
    #     new_bins = [[0,1],[2]]
    #     ts1 = time.mktime(time.strptime(time1, "%a %b %d %H:%M:%S %Y"))
    #     ts2 = time.mktime(time.strptime(time2, "%a %b %d %H:%M:%S %Y"))
    #     ts3 = time.mktime(time.strptime(time3, "%a %b %d %H:%M:%S %Y"))
    #     filter_trips = [{'data': {'start_ts': ts1}},
    #                     {'data': {'start_ts': ts2}},
    #                     {'data': {'start_ts': ts3}}]
    #     self.assertEqual(eamtg.requested_trips_ab_cutoff(new_bins, filter_trips),([0, 2], [1]))
    #
    #
    # def test_requested_trips_bl_cutoff(self):
    #
    #     # requested_trips_bl_cutoff(sim)
    #     fake_trip_collect = []
    #     trip1 = pd.DataFrame(data=([[-122.41925243091958,-122.42140476014033],[37.77938521735944,37.78194309045273]]),
    #                       columns=[['start_loc','end_loc'],
    #                                ['coordinates','coordinates']])
    #     fake_trip_collect.append(trip1)
    #     trip2 = pd.DataFrame(data=([[-122.41925243091958, -122.42093683661327], [37.77938521735944, 37.782278693221016]]),
    #                        columns=[['start_loc', 'end_loc'],
    #                                 ['coordinates', 'coordinates']])
    #     fake_trip_collect.append(trip2)
    #     trip3 = pd.DataFrame(data=([[-123.41925243091958,-122.41912876839925],[37.77938521735944,37.77766191670088]]),
    #                       columns=[['start_loc','end_loc'],
    #                                ['coordinates','coordinates']])
    #     fake_trip_collect.append(trip3)
    #     sim = similarity.similarity(fake_trip_collect,100)
    #     print(sim.below_cutoff)
    #     # print(bl_trip_ls)
    #
    #
    #
    #     # df = pd.DataFrame(columns=[['start_loc','end_loc'],['coordinates','coordinates']])
    #
    #     # print(df)
    #     # df1 = pd.DataFrame(np.ra]ndom.randint(0, 150, size=(4, 6)),
    #     #                    columns=[['python', 'python', 'math', 'math', 'En', 'En'],
    #     #                             ['期中', '期末', '期中', '期末', '期中', '期末']])
    #     # print(df1.python['期中'])

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()

