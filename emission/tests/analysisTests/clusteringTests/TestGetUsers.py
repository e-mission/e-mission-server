from future import standard_library
standard_library.install_aliases()
import unittest
import emission.analysis.modelling.tour_model.get_users as gu
import emission.analysis.modelling.tour_model.data_preprocessing as preprocess

import emission.tests.common as etc

class TestGetUsers(unittest.TestCase):
    # def setUp(self):
    #
    # def tearDown(self):


    def test_valid_user(self):
        user = [self.testUUID]
        radius = 100
        trips = preprocess.read_data(user)
        filter_trips = preprocess.filter_data(trips,radius)
        valid = gu.valid_user(filter_trips, trips)
        # assertEqual


    def test_get_user_ls(self):
        all_users = [self.testUUID]
        radius = 100
        user_ls,valid_user_ls = gu.get_user_ls(all_users, radius)
        # assertEqual

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()


