from abc import ABC, abstractmethod
from emission.simulation.fake_user import FakeUser
from emission.simulation.error import AddressNotFoundError
import requests

class Client(ABC):
    def __init__(self):
        super().__init__()
    
    @abstractmethod
    def create_fake_user(self, config):
        pass 
    @abstractmethod
    def _parse_user_config(self, config):
        pass  

class EmissionFakeDataGenerator(Client):
    def __init__(self, config):
        #TODO: Check that the config object has keys: emission_server_base_url, register_user_endpoint, user_cache_endpoint
        self._config = config
        self._user_factory = FakeUser

    def create_fake_user(self, config):
        #TODO: parse the config object
        uuid = self._register_fake_user(config['email'])
        config['uuid'] = uuid
        config['upload_url'] = self._config['emission_server_base_url'] + self._config['user_cache_endpoint']
        return self._user_factory(config)

    def _register_fake_user(self, email):
        data = {'user': email}
        url = self._config['emission_server_base_url'] + self._config['register_user_endpoint'] 
        r = requests.post(url, json=data)
        r.raise_for_status()
        uuid = r.json()['uuid']
        #TODO: This is a hack to make all the genereated entries JSON encodeable. 
        #Might be a bad Idead to stringify the uuid. For instance, 
        # the create_entry function expects uuid of type UUID
        return str(uuid)

    def _parse_user_config(self, config):
        #TODO: This function shoudl be used to parser user config object and check that the paramaters are valid.
        try: 
            locations = config['locations']
        except KeyError:
            print("You must specify a set of addresses")
            raise AddressNotFoundError

        #check that all addresses are supported by the trip planner software
        #for address in addresses:
        #    if not self._trip_planer_client.has_address(address):
        #        message = ("%s, is not supported by the Trip Planer", address) 
        #        raise AddressNotFoundError(message, address)

        #check that all teh transition probabilites for every address adds up to one

        
