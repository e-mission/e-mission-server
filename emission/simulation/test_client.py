import unittest
from client import EmissionFakeDataGenerator
from error import AddressNotFoundError

class TestClientMethods(unittest.TestCase):
    def setUp(self):
        client_config = {
            'emission_server_base_url': 'http://localhost:8080',
            'register_user_endpoint': '/profile/create',
             'user_cache_endpoint': '/usercache'
        }
        self.emission_data_generator = EmissionFakeDataGenerator(client_config)

    def test_register_fake_user(self):
        email = 'register_fake@user'
        uuid = self.emission_data_generator._register_fake_user(email)
        print(uuid)

    def test_parse_user_config(self):
        pass
        #TODO Test for invalid modes as well, modes supported by the OTP server 
        
        #self.assertRaises(AddressNotFoundError, lambda: self.emission_data_generator._parse_user_config(config_invalid_address))

if __name__ == '__main__':
    unittest.main()
        