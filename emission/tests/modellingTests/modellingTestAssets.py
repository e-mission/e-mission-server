import random
from typing import Optional, Tuple, List, Dict
from uuid import UUID
import emission.analysis.modelling.trip_model.greedy_similarity_binning as eamtg
import emission.core.wrapper.confirmedtrip as ecwc
import emission.core.common as ecc
import emission.core.wrapper.entry as ecwe
import time 
import math

def generate_random_point():
    """Generate a completetly random point valid WGS84 latitiude and longtidude.   
    CAUTION : In order to save trips, GeoJSON requires points in [lon,lat] format"""    
    lat=random.uniform(-90,90)
    lon=random.uniform(-180,180)
    return [lon,lat]

def generate_nearby_random_points(ref_coords,threshold):
    """
    Generate valid WGS84 latitiude and longtidude in threshold(m) proximity to
    ref coordinates.
    """
    #convert given threshold in m to approx WGS84 coord dist.
    thresholdInWGS84 = threshold* (0.000001/0.11)
    
    #generate a random coordinate in threshold's limit around the ref points. OR we 

    # for eg, ref point is 0,0 and threshold is  100m , so we generate a radius from 0 to 50, say 34 
    # in this example. A random radius is also generted from 0 to 360,say 0. We then take 34 step along x axis direction 
    # till radius length to get our new point, (34,0). When this function is called the next time to generate a point
    #that has to be binned with previous one, we again generate r and theta , say 24 , 180 this time. 
    # Now this new point is at (-24,0). Both these points are within threshold (100 in this case)limit and therefore will 
    #be binned together.
    radius=random.uniform(0,thresholdInWGS84/2)
    theta=random.uniform(0,2*math.pi)
    dx = radius * math.cos(theta)
    dy = radius * math.sin (theta)
    #This basically gives a way to sample a point from within a circle of radius thresholdInWGS84/2 
    # around the ref. point.  
    return [ref_coords[0] + dy , ref_coords[1] + dx]

def generate_trip_coordinates(
    points_list: list[float],
    ref_coords, 
    insideThreshold: bool,
    threshold_meters: float, 
    ) -> Tuple[float, float]:
    """generates trip coordinate data to use when mocking a set of trip data.i
    If the coordinate generated  is to be binned together, it is generated in proximity of
    the previous points in the points_list. Otherwise, if this point is not to be binned together,
    keep generating a random trip unless we find one that would not bin with previously
    accepeted trips.

    :param points_list: list of all the previoushlt selected points 
    :param within_threshold: is this point  supposed to be within some distance threshold
    :param threshold_meters: the distance threshold, in meters
    :return: generated coordinate pairs sampled in a 
             circle from some coordinates up to some threshold
    """
    # if the point is to be generated within a threshold and it's not the first point
    if insideThreshold and points_list:
        # if  no ref. coordinates are provided, use any previously accepted point as ref.
        if ref_coords == None:
            ref_coords=random.choice(points_list)
        # generate a new point in threshold proximity to ref. point
        new_point = generate_nearby_random_points(ref_coords, threshold_meters)    
    else: # If point need not be in the threshold  OR if its the first point we are generating, then
         #Generate random coordinates if no reference coords were provided  
        if ref_coords == None:            
            new_point = generate_random_point()
        else:
         # if ref coordinate are provided, use them as the startisng point. 
            new_point = ref_coords
        # If the newly generated new_point ( be it when ref_coords given or not given) is not more 
        # than threshold_meters away from all the previously accepted points, keep generating new_point        # 
        while not all(ecc.calDistance(new_point, pt) > threshold_meters for pt in points_list):
            new_point = generate_random_point()
    return new_point


def extract_trip_labels(trips: List[ecwc.Confirmedtrip]) -> Dict:
    """
    helper to build the `label_data` argument for the generate_mock_trips
    function below. reads all entries from a list of Confirmedtrip entries.

    :param trips: the trips to read from
    :return: label_data
    """
    keys = ['mode_confirm', 'purpose_confirm', 'replaced_mode']
    result = {k: set() for k in keys}
    for k in keys:
        for t in trips:
            entry = t['data']['user_input'].get(k)
            if entry is not None:
                result[k].add(entry) 
    for k in result.keys():
        result[k] = list(result[k])
    return result


def sample_trip_labels(
    mode_labels, 
    purpose_labels,
    replaced_mode_labels,
    mode_weights=None, 
    purpose_weights=None,
    replaced_mode_weights=None):
    """samples trip labels

    :param mode_labels: labels for mode_confirm
    :param purpose_labels: labels for purpose_confirm
    :param replaced_mode_labels: labels for replaced_mode
    :param mode_weights: sample weights, defaults to None, see random.choices "weights"
    :param purpose_weights: sample weights, defaults to None for uniform sampling
    :param replaced_mode_weights: sample weights, defaults to None
    :return: sampled trip labels
    """
    user_inputs = [
        ('mode_confirm', mode_labels, mode_weights),
        ('replaced_mode', replaced_mode_labels, replaced_mode_weights),
        ('purpose_confirm', purpose_labels, purpose_weights)
    ]

    result = {}
    for key, labels, weights in user_inputs:
        if len(labels) > 0:
            if weights is None:
                weights = [1.0 / len(labels) for i in range(len(labels))]
            samples = random.choices(population=labels,k=1,weights=weights)
            result[key] = samples[0]

    return result


def build_mock_trip(
    user_id: UUID, 
    origin, 
    destination, 
    labels: Optional[Dict] = {}, 
    start_ts: Optional[float] = None,
    end_ts: Optional[float] = None) -> ecwc.Confirmedtrip:
    """repackages mock data as a Confirmedtrip Entry type

    NOTE: these mock objects **do not** include all fields. see Trip and Confirmedtrip
    classes for the complete list and expand if necessary.

    :param user_id: the user id UUID
    :param origin: trip origin coordinates
    :param destination: trip destination coordinates
    :param labels: user labels for the trip, optional, default none
    :param start_ts: optional timestamp for trip start, otherwise NOW
    :param end_ts: optional timestamp for trip end, otherwise NOW
    :return: a mock Confirmedtrip entry
    """
    start_ts = start_ts if start_ts is not None else time.time()
    end_ts = end_ts if end_ts is not None else time.time()
    key = "analysis/confirmed_trip"
    data = {
        "start_ts": start_ts,
        "start_loc": {
            "type": "Point",
            "coordinates": origin
        },
        "end_ts": end_ts,
        "end_loc": {
            "type": "Point",
            "coordinates": destination
        },
        "user_input": labels
    }

    return ecwe.Entry.create_fake_entry(user_id, key, data, write_ts=time.time())


def generate_mock_trips(
    user_id, 
    trips,
    threshold,
    trip_part='od',
    origin=None,
    destination=None,
    label_data = None, 
    within_threshold = None,
    start_ts: None = None,
    end_ts: None = None,
    has_label_p = 1.0,
    seed = 0):
    """mocking function that generates multiple trips for a user. some are sampled 
    within a threshold from the provided o/d pair, and some have labels. some other
    ones can be sampled to appear outside of the threshold of the o/d locations.

    label_data is an optional dictionary with labels and sample weights, for example:
    {
        "mode_confirm": ['walk', 'bike'],
        "replaced_mode": ['drive', 'tnc'],
        "purpose_confirm": ['home', 'work'],
        "mode_weights": [0.8, 0.2],
        "replaced_mode_weights": [0.4, 0.6],
        "purpose_weights": [0.1, 0.9]
    }

    weights entries are optional and result in uniform sampling.

    :param user_id: user UUID
    :param trips: number of trips
    :param trip_part: when mock trips are generated, coordinates of this part of 
                      the trips will be within the threshold. trip_part can take one
                      among the four values:
                    1. '__' ->(None, meaning NEITHER origin nor destination of any trip will lie 
                     within the mentioned threshold when trips are generated),        
                    2. 'o_' ->(origin, meaning ONLY origin of m trips will lie within the mentioned 
                     threshold when trips are generated),        
                    3. '_d' ->(destination),meaning ONLY destination of m trips will lie within the 
                     mentioned threshold when trips are generated)        
                    4. 'od' ->(origin and destination,meaning BOTH origin and destination of m trips
                     will lie within the mentioned threshold when trips are generated)
    :param origin : reference point for trip origin generally
    :param destination : reference point for trip origin generally
    :param label_data: dictionary of label data, see above, defaults to None
    :param within_threshold: number of trips that should fall within the provided
           distance threshold in m
    :param threshold: distance threshold in WGS84 for sampling
    :param has_label_p: probability a trip has labels, defaults to 1.0
    :param seed: random seed, defaults to 0
    :return: randomly sampled trips
    """
    
    random.seed(seed)
    within = within_threshold if within_threshold is not None else trips
    trips_within_threshold = [i < within for i in range(trips)]
    result = []
    origin_points=[]
    destination_points=[]    

    # generate 'trip' number of points based on which among 'o' (Origin) ,'d' (Destination) or
    # 'od' (Origin-Destination) or '__' (None) should be in threshold proximity to each other. 
    for within in trips_within_threshold:
        origin_points.append(generate_trip_coordinates(origin_points, origin, insideThreshold= (trip_part[0] == 'o' and within), threshold_meters= threshold))
        destination_points.append(generate_trip_coordinates(destination_points, destination, insideThreshold=(trip_part[1] == 'd' and within), threshold_meters=threshold))

    for o,d in zip(origin_points,destination_points):    
        labels = {} if label_data is None or random.random() > has_label_p \
            else sample_trip_labels(
            mode_labels=label_data.get('mode_confirm'),
            replaced_mode_labels=label_data.get('replaced_mode'),
            purpose_labels=label_data.get('purpose_confirm'),
            mode_weights=label_data.get('mode_weights'),
            replaced_mode_weights=label_data.get('replaced_mode_weights'),
            purpose_weights=label_data.get('purpose_weights')
        )
        trip = build_mock_trip(user_id, o, d, labels, start_ts, end_ts)
        result.append(trip)
        
    random.shuffle(result) 
    return result


if __name__ == '__main__':
    label_data = {
        "mode_confirm": ['walk', 'bike', 'drive'],
        "purpose_confirm": ['work', 'home', 'school'],
        "replaced_mode": ['walk', 'bike', 'drive']
    }
    result = generate_mock_trips('joe-bob', 14, [0, 0], [1,1],'od', label_data, 6)
    for r in result:
        print(r)