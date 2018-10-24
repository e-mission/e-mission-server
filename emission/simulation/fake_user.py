
from abc import ABC, abstractmethod
import datascience 
from prob140 import MarkovChain 
import numpy as np
import datetime
import arrow 

#emission imports
import emission.core.wrapper.user as ecwu
from emission.net.ext_service.otp.otp import OTP, PathNotFoundException

#TODO: make an astrach class FakeUser and createa concrete implementation called EmssionFakeUser

class FakeUser:
    def __init__(self, config={}):
        self._email = config['email']
        self._config = config
        self._uuid = ecwu.User.register(self._email).uuid # this must be the uuid from the database
        self._time_object = arrow.utcnow()
        self._trip_planer_client = OTP
        self._current_state = config['initial_state']
        self._markov_model = self._create_markow_model(config) #MarkovChain(config['addresses'], config['transition_probabilities'])
        self._path = [self._current_state]
        self._lable_to_coordinate_map = self._create_lable_to_coordinate_map(config)
        self._trip_to_mode_map = self._create_trip_to_mode_map(config)

    def take_trip(self):
        #TODO: If we have already completed a trip, we could in theory cache the location data 
        # we get from Open Trip Planner and only modify the timestamps. 
       #get next state
        curr_loc = self._current_state
        next_loc = self._markov_model.simulate_path(self._current_state, 1)[-1]
       # if next state is current state return empty list
        if next_loc == self._current_state:
            print('>> Staying at', curr_loc)
            return []
       # else, find point withing a certain radius of current state and end state
        curr_coordinate = self._lable_to_coordinate_map[curr_loc] 
        next_coordinate = self._lable_to_coordinate_map[next_loc]
       #create an new OTP Trip
        trip_planer_client = self._create_new_otp_trip(curr_coordinate, next_coordinate)

       #TODO: we could catch PathNotFound Exception and return an empty list? 
        # get the measurements along the route. 
        print('>> Traveling from', curr_loc,'to', next_loc)
        measurements = trip_planer_client.get_measurements_along_route(self._uuid)
      
       #update current_state 
       #deal with timing TODO: make sure the next trip starts at least 1 hour after the last trip ended.
        if len(measurements) > 0:
            end_time_last_trip = measurements[-1].data.ts
            self._update_time(end_time_last_trip)
            self._current_state = next_loc
            self._path.append(next_loc)

        return measurements
    
    def _create_new_otp_trip(self, curr_coordinate, next_coordinate):
        try:
            mode = self._trip_to_mode_map[(curr_coordinate, next_coordinate)]
        except KeyError:
            mode = self._config['default_mode']

        date = "%s-%s-%s" % (self._time_object.month, self._time_object.day, self._time_object.year)
        time = "%s:%s" % (self._time_object.hour, self._time_object.minute)
        
        #TODO: Figure out how we should set bike
        return self._trip_planer_client(curr_coordinate, next_coordinate, mode, date, time, bike=True)
    
    def _update_time(self, prev_trip_end_time):
        self._time_object = arrow.get(prev_trip_end_time).shift(hours=+3)
    
    def _create_markow_model(self, config):
        lables = [elem['lable'].lower() for elem in config['locations']]
        transitions_probabilities = config['transition_probabilities']
        return MarkovChain(lables, transitions_probabilities)

    def _create_lable_to_coordinate_map(self, config):
        locations = config['locations']
        new_map = {}
        for loc in locations:
            new_map[loc['lable']] = tuple(loc['coordinate'])

        return new_map
    
    def _create_trip_to_mode_map(self, config):
        new_map = {}
        for k, v in config['modes'].items():
            for edge in v:
                new_map[tuple(edge)] = k
        return new_map

        