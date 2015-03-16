import get_trips
from common import store_trip_in_db
#import Profiles

def alternative_trips(trip_list):
    '''
    store trips to database
    '''
    for trip in trip_list:
        store_trip_in_db(trip)
    

def get_bare_trips(user_ID, filter_queries):
    '''
    get a list of user trips, using get_trips logic (filter? will produce canonical trip?)
    
    '''
    return

def get_user_ids(filter_queries):
    '''
    get a list of user ids
    '''

    return

def get_alternative_trips(user_ID, trip_ID):
    '''
    return an array of Trip objects. Call Perturbation Module here (perturb times)
    Will query at appropriate times.
    '''
    return

get_user_ids(filter_queries)
bare_trips = get_bare_trips()
augment_trips()
