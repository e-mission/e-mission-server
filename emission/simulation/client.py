from abc import ABC, abstractmethod
from emission.simulation.fake_user import FakeUser
from emission.simulation.error import AddressNotFoundError

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
    def __init__(self):
        self._user_factory = FakeUser

    def create_fake_user(self, config):
        #TODO: parse the config object
        return self._user_factory(config)

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

        