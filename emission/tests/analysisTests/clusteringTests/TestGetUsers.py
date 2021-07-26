from future import standard_library
standard_library.install_aliases()
import unittest
import emission.analysis.modelling.tour_model.get_users as gu
import emission.analysis.modelling.tour_model.data_preprocessing as preprocess
import emission.tests.common as etc
import json
import bson.json_util as bju
import copy


class TestGetUsers(unittest.TestCase):
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


    def test_valid_user(self):
        trips = preprocess.read_data(self.user)
        filter_trips = preprocess.filter_data(trips,self.radius)
        # the user has 8 labeled trips, >50% of trips are labeled
        valid = gu.valid_user(filter_trips, trips)
        self.assertEqual(valid,False)
        for i in range(2):
            filter_trips.append(copy.copy(filter_trips[0]))
        # now the user has 10 labeled trips, >50% of trips are labeled
        valid = gu.valid_user(filter_trips, trips)
        self.assertEqual(valid,True)

    def test_get_user_ls(self):
        # only 1 invalid user
        user_ls,valid_user_ls = gu.get_user_ls([self.user], self.radius)
        self.assertEqual(len(user_ls),1)
        self.assertEqual(len(valid_user_ls),0)


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()


