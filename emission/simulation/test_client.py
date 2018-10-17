import unittest
from client import EmissionFakeDataGenerator
from error import AddressNotFoundError

class TestClientMethods(unittest.TestCase):
    def setUp(self):
        self.emission_data_generator = EmissionFakeDataGenerator()

    def test_create_new_fake_user(self):
        config = {
            "radius" : ".1",
	        "starting centroids" :
	        {
                "2703 Hallmark Dr Belmont" : 100,
                "AT&T Park" : 20, 
                "Skyline Wilderness Park" : 10, 
                "UC Berkeley" : 50

	        },
	        "ending centroids" :
            {
                "Skyline Wilderness Park" : 10,
                "AT&T Park" : 100, 
                "UC Berkeley" : 75,
                "2703 Hallmark Dr Belmont" : 100

            },
            "modes" : 
            {
                "CAR" : 100, 
                "WALK" : 10, 
                "BICYCLE" : 1, 
                "TRANSIT" : 50
            },
	        "number of trips" : 50
        }
        email = 'test_fake_user'
        user = self.emission_data_generator.create_fake_user(email, config)
        self.assertEqual(user._email, email)

    def test_parse_user_config(self):
        config = {
            "radius" : ".1",
	        "starting centroids" :
	        {
                "2703 Hallmark Dr Belmont" : 100,
                "AT&T Park" : 20, 
                "Skyline Wilderness Park" : 10, 
                "UC Berkeley" : 50

	        },
	        "ending centroids" :
            {
                "Skyline Wilderness Park" : 10,
                "AT&T Park" : 100, 
                "UC Berkeley" : 75,
                "2703 Hallmark Dr Belmont" : 100

            },
            "modes" : 
            {
                "CAR" : 100, 
                "WALK" : 10, 
                "BICYCLE" : 1, 
                "TRANSIT" : 50
            },
	        "number of trips" : 50
        }

        user_1 = self.emission_data_generator.create_fake_user('test_fake_user', config)

        config_invalid_address = {
            "radius" : ".1",
	        "starting centroids" :
	        {
                "iasdfad" : 100,
                "AT&T Park" : 20, 
                "Skyline Wilderness Park" : 10, 
                "UC Berkeley" : 50

	        },
	        "ending centroids" :
            {
                "Skyline Wilderness Park" : 10,
                "AT&T Park" : 100, 
                "UC Berkeley" : 75,
                "2703 Hallmark Dr Belmont" : 100

            },
            "modes" : 
            {
                "CAR" : 100, 
                "WALK" : 10, 
                "BICYCLE" : 1, 
                "TRANSIT" : 50
            },
	        "number of trips" : 50
        }
        
        self.assertRaises(AddressNotFoundError, lambda: self.emission_data_generator._parse_user_config(config_invalid_address))

if __name__ == '__main__':
    unittest.main()
        