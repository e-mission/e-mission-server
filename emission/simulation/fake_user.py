
from abc import ABC, abstractmethod
import datascience 
from prob140 import MarkovChain 
import numpy as np

#make abstract
class FakeUser:
    def __init__(self, email, config={}):
        self._email = email
        self._config = config
        self._current_state = config['initial_state']
        self._markov_model = MarkovChain(config['addresses'], config['transition_probabilities'])
        self._path = [self._current_state]
        self._address_to_coordinate_map = {}

    def take_trip(self):
       #get next state
        curr_address = self._current_state
        next_address = self._markov_model.simulate_path(self._current_state, 1)[-1]
       # if next state is current state return empty list
        if next_address == self._current_state:
            return []
       # else, fins point withing a certain radius of current state and end state
        try:
            curr_coordinate = self._address_to_coordinate_map[curr_address] 
        except KeyError:
            curr_coordinate = (0,0) # geocode address, wait this has already been done in the input parser stage?
        
        try:
            next_coordinate = self._address_to_coordinate_map[next_address] 
        except KeyError:
            next_coordinate = (1,0) 
       #create an new OTP object

       # deal with timing TODO: make sure the next trip starts at least 10 minutes after the last trip ended. 
       # get the measurements along the route. call OTP.get_measurements 
       #update current_state
        self._current_state = next_address
        self._path.append(next_address)
        return []  
        