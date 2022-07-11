import random
from typing import Optional, Tuple, List, Dict
from uuid import UUID
import emission.analysis.modelling.trip_model.trip_model as eamtm
import emission.core.wrapper.confirmedtrip as ecwc

import emission.core.wrapper.entry as ecwe
import time 
import math


def generate_trip_coordinates(
    ref_coords: Tuple[float, float], 
    within_threshold: bool,
    threshold: float, 
    max: float = 0.1  # approx. 10km in WGS84 
    ) -> Tuple[float, float]:
    """generates trip coordinate data to use when mocking a set of trip data.

    :param ref_coords: reference coordinates to use as the center of the sampling circle
    :param within_threshold: how many of these trips are within some distance threshold
    :param threshold: the distance threshold, in WGS84
    :param max: max distance, in WGS84, defaults to 0.1 (approx. 10km)
    :return: generated coordinate pairs sampled in a 
             circle from some coordinates up to some threshold
    """
    angle = 2 * math.pi * random.random()
    radius_threshold = threshold / 2
    radius = random.uniform(0, radius_threshold) if within_threshold else random.uniform(radius_threshold, max)
    x = radius * math.cos(angle) + ref_coords[0]
    y = radius * math.sin(angle) + ref_coords[1]
    return (x, y)


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
    origin, 
    destination, 
    label_data = None, 
    within_threshold = None,
    start_ts: None = None,
    end_ts: None = None,
    threshold = 0.01,
    max = 0.1, 
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
    :param origin: origin coordinates
    :param destination: destination coordinates
    :param label_data: dictionary of label data, see above, defaults to None
    :param within_threshold: number of trips that should fall within the provided
           distance threshold in degrees WGS84, defaults to None
    :param threshold: distance threshold in WGS84 for sampling, defaults to 0.01
    :param max: maximum distance beyond the threshold for trips sampled that
                are not within the threshold, defaults to 0.1 degrees WGS84
    :param has_label_p: probability a trip has labels, defaults to 1.0
    :param seed: random seed, defaults to 0
    :return: randomly sampled trips
    """
    
    random.seed(seed)
    within = within_threshold if within_threshold is not None else trips
    trips_within_threshold = [i < within for i in range(trips)]
    result = []
    for within in trips_within_threshold:
        o = generate_trip_coordinates(origin, within, threshold, max)
        d = generate_trip_coordinates(destination, within, threshold, max)
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
    result = generate_mock_trips('joe-bob', 14, [0, 0], [1,1], label_data, 6)
    for r in result:
        print(r)