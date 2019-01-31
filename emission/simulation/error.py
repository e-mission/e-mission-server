class Error(Exception):
    pass 

class AddressNotFoundError(Error):
    """ Exception raised for Addresses that are not found in the trip planner client.
    """
    def __init__(self, message, address):
        self.message = message
        self.address = address 

