import unittest
from fake_user import FakeUser
from error import AddressNotFoundError
import datascience 
import prob140
import numpy as np

class TestFakeUserMethods(unittest.TestCase):
    def setUp(self):
        self.config = {
            "email" : 'my_fake_user',
            "uuid" : '124',
            "upload_url" : 'http://localhost:8080/usercache/put',
	        "locations" : 
	        [
               {
                    'label': 'home',
                    'coordinate': [37.77264255,-122.399714854263]
                },

                {
                    'label': 'work',
                    'coordinate': [37.42870635,-122.140926605802]
                },
                {
                    'label': 'family',
                    'coordinate': [37.87119, -122.27388]
                }
            ],
            "transition_probabilities":
            [
                np.random.dirichlet(np.ones(3), size=1)[0],
                np.random.dirichlet(np.ones(3), size=1)[0],
                np.random.dirichlet(np.ones(3), size=1)[0]
            ],
            "modes" : 
            {
                "CAR" : [['home', 'family']],
                "TRANSIT" : [['home, work'], ['work', 'home']]  
            },

            "default_mode": "CAR",
	        "initial_state" : "home",
            "radius" : ".1"
        }
        #email = 'test_fake_user'
        self.fake_user = FakeUser(self.config)
    
    def test_init(self):
        self.assertEqual(self.fake_user._email, 'my_fake_user')

    def test_upload_to_server(self):
        self.fake_user.take_trip()
        self.fake_user.upload_measurments()

    def test_take_trip(self):
        self.assertEqual(self.fake_user._current_state, self.config['initial_state'])
        measurements = self.fake_user.take_trip()
        #print(self.fake_user._current_state)


    def test_take_many_trips(self):
        for _ in range(10):
            self.fake_user.take_trip()
            print(self.fake_user._current_state)

        print(self.fake_user._path)
    
    def test_trip_to_mode_map(self):
        new_map = self.fake_user._create_trip_to_mode_map(self.config)
        edges = []
        for k,v in self.config['modes'].items():
            for edge in v:
                edges.append(tuple(edge))
        
        for edge in edges:
            self.assertTrue(edge in new_map.keys())

    def test_create_otp_trip(self):
        home = (37.77264255,-122.399714854263)
        work = (37.42870635,-122.140926605802)
        otp = self.fake_user._create_new_otp_trip(home, work)
        measurements = otp.get_measurements_along_route(self.fake_user._uuid)
        print(len(measurements))



if __name__ == '__main__':
    unittest.main()
        