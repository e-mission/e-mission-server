
from abc import ABC, abstractmethod
import datascience 
from prob140 import MarkovChain 
import numpy as np
import datetime
import arrow 
import requests
#emission imports
import emission.core.wrapper.user as ecwu
from emission.net.ext_service.otp.otp import OTP, PathNotFoundException

class FakeUser:
    """
    Fake user class used to genreate synthetic data.
    """
#TODO: Make FakeUser an abstract class and create a concrete implementation called EmissionFakeUser
    def __init__(self, config={}):
        self._config = config
        self._email = config['email']
        self._uuid = config['uuid'] 
        # We need to set the time of ther user in the past to that the pipeline can find the entries.
        self._time_object = arrow.utcnow().shift(years=-1) 
        self._trip_planer_client = OTP
        self._current_state = config['initial_state']
        self._markov_model = self._create_markow_model(config) #MarkovChain(config['addresses'], config['transition_probabilities'])
        self._path = [self._current_state]
        self._label_to_coordinate_map = self._create_label_to_coordinate_map(config)
        self._trip_to_mode_map = self._create_trip_to_mode_map(config)
        self._measurements_cache = []

    def take_trip(self):
        #TODO: If we have already completed a trip, we could potentially cache the location data 
        # we get from Open Trip Planner and only modify the timestamps next time we take the same trip. 
        curr_loc = self._current_state
        next_loc = self._markov_model.simulate_path(self._current_state, 1)[-1]

       #If the next location is the same as the current location, return an empty list
        if next_loc == self._current_state:
            print('>> Staying at', curr_loc)
            return []

        curr_coordinate = self._label_to_coordinate_map[curr_loc] 
        next_coordinate = self._label_to_coordinate_map[next_loc]

        trip_planer_client = self._create_new_otp_trip(curr_coordinate, next_coordinate, curr_loc, next_loc)

        # Get the measurements along the route. This includes location entries
        # and a motion entry for each section.
       #TODO: If get_measurements_along_route returns a PathNotFound Exception, we should catch this and return an empty list(?) 
        print('>> Traveling from', curr_loc,'to', next_loc, '| Mode of transportation:', trip_planer_client.mode)
        measurements = trip_planer_client.get_measurements_along_route(self._uuid)
      
       # Here we update the current state 
       # We also update the time_object to make sure the next trip starts at a later time 
        if len(measurements) > 0:
            end_time_last_trip = measurements[-1].data.ts
            self._update_time(end_time_last_trip)
            self._current_state = next_loc
            self._path.append(next_loc)
            #Update measurements cache
            self._measurements_cache += measurements
            #TODO: if the user cache has more than 5000 entries notify the user so they can sync the data. 

        return measurements
    def sync_data_to_server(self):
        #Remove the _id field
        measurements_no_id = [self._remove_id_field(entry) for entry in self._measurements_cache]
        #Send data to server
        data = {
            'phone_to_server': measurements_no_id,
            'user': self._email
        }

        r = requests.post(self._config['upload_url'], json=data)

        #Check if sucessful
        if r.ok:
            self._flush_cache()
            print("%d entries were sucessfully synced to the server" % len(measurements_no_id))
        else:
            print('Something went wrong when trying to sync your data. Try again or use save_cache_to_file to save your data.')
            print(r.content)

    def _create_new_otp_trip(self, curr_coordinate, next_coordinate, cur_loc, next_loc):
        try:
            mode = self._trip_to_mode_map[(cur_loc, next_loc)]
        except KeyError:
            mode = self._config['default_mode']

        date = "%s-%s-%s" % (self._time_object.month, self._time_object.day, self._time_object.year)
        time = "%s:%s" % (self._time_object.hour, self._time_object.minute)
        
        #TODO: Figure out ho we should set bike
        return self._trip_planer_client(curr_coordinate, next_coordinate, mode, date, time, bike=True)
    
    def _update_time(self, prev_trip_end_time):
        # TODO: 3 hours is an arbritrary value. Not sure what makes sense. 
        self._time_object = arrow.get(prev_trip_end_time).shift(hours=+3)
    
    def _create_markow_model(self, config):
        labels = [elem['label'].lower() for elem in config['locations']]
        transitions_probabilities = config['transition_probabilities']
        return MarkovChain(labels, transitions_probabilities)

    def _create_label_to_coordinate_map(self, config):
        locations = config['locations']
        new_map = {}
        for loc in locations:
            new_map[loc['label']] = tuple(loc['coordinate'])

        return new_map
    
    def _create_trip_to_mode_map(self, config):
        new_map = {}
        for k, v in config['modes'].items():
            for edge in v:
                new_map[tuple(edge)] = k
        return new_map
    
    @staticmethod
    def _remove_id_field(entry):
        copy = entry.copy()
        del copy['_id']
        return copy
    
    def _flush_cache(self):
        self._measurements_cache = []

        