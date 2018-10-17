import unittest
from fake_user import FakeUser
from error import AddressNotFoundError
import datascience 
import prob140
import numpy as np

class TestFakeUserMethods(unittest.TestCase):
    def setUp(self):
        self.config = {
            "radius" : ".1",
	        "addresses" :
	        [
                "2703 Hallmark Dr Belmont", 
                "AT&T Park", 
                "Skyline Wilderness Park", 
                "UC Berkeley",
            ],
            "transition_probabilities":
            [
                np.random.dirichlet(np.ones(4), size=1)[0],
                np.random.dirichlet(np.ones(4), size=1)[0],
                np.random.dirichlet(np.ones(4), size=1)[0],
                np.random.dirichlet(np.ones(4), size=1)[0],
            ],
            "modes" : 
            {
                "CAR" : 100, 
                "WALK" : 10, 
                "BICYCLE" : 1, 
                "TRANSIT" : 50
            },
	        "initial_state" : "2703 Hallmark Dr Belmont" 
        }
        email = 'test_fake_user'
        self.fake_user = FakeUser(email, self.config)
    
    def test_init(self):
        self.assertEqual(self.fake_user._email, 'test_fake_user')

    def test_take_trip(self):
        self.assertEqual(self.fake_user._current_state, self.config['initial_state'])
        measurements = self.fake_user.take_trip()
        #print(self.fake_user._current_state)
    
    def test_take_many_trips(self):
        for _ in range(100):
            self.fake_user.take_trip()
            print(self.fake_user._current_state)

        print(self.fake_user._path)

if __name__ == '__main__':
    unittest.main()
        