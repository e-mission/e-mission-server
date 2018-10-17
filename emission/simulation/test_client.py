import unittest
from client import EmissionFakeDataGenerator
from error import AddressNotFoundError

class TestClientMethods(unittest.TestCase):
    def setUp(self):
        self.emission_data_generator = EmissionFakeDataGenerator()

    def test_create_new_fake_user(self):
        config = {
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
        user = self.emission_data_generator.create_fake_user(email, config)
        self.assertEqual(user._email, email)

    def test_parse_user_config(self):
        config = {
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

        user_1 = self.emission_data_generator.create_fake_user('test_fake_user', config)

        config_invalid_address = {
            "radius" : ".1",
	        "addresses" :
	        [
                "adsfasdfmont", 
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
        #TODO Test for invalid modes as well, modes supported by the OTP server 
        
        self.assertRaises(AddressNotFoundError, lambda: self.emission_data_generator._parse_user_config(config_invalid_address))

if __name__ == '__main__':
    unittest.main()
        