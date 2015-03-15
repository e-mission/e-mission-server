import get_trips
import Trip
#import Profiles

def alternative_trips(trip_list):
    '''
    store trips to database
    '''
    return

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
    routes = directions["routes"]
    trips = []
    for route in routes:
        legs = []
        for leg in route["legs"]:
            leg = Leg("some_trip_id")
            leg.starting_point = (leg["steps"][0]["start_location"]["lat"], leg["steps"][0]["start_location"]["lng"])
            leg.ending_point = (leg["steps"][-1]["end_location"]["lat"], leg["steps"][-1]["end_location"]["lng"])
            leg.mode = step["travel_mode"]
            totalDuration = 0
            totalDistance = 0
            for step in leg:
                totalDuration += step["duration"]["value"]
                totalDistance += step["distance"]["value"]
            legs.push(leg)

    return

get_user_ids(filter_queries)
bare_trips = get_bare_trips(
augment_trips()
