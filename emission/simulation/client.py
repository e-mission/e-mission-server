from abc import ABC, abstractmethod
from emission.simulation.fake_user import FakeUser
from emission.simulation.error import AddressNotFoundError
#TODO: the trip planner should be kept in the FakeUser class, not in the client
from emission.net.ext_service.otp.otp import OTP

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
        self._trip_planer_client = OTP

    def create_fake_user(self, config):
        #parse the config
        return self._user_factory(config)

    def _parse_user_config(self, config):

        #check that config has initial state location and that the address is in the addresses list 

        try: 
            addresses = config['addresses']
        except KeyError:
            print("You must specify a set of addresses")
            raise AddressNotFoundError

        #check that all addresses are supported by the trip planner software
        #for address in addresses:
        #    if not self._trip_planer_client.has_address(address):
        #        message = ("%s, is not supported by the Trip Planer", address) 
        #        raise AddressNotFoundError(message, address)

        #check that all teh transition probabilites for every address ads up to one

        