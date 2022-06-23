import random
from typing import Tuple, List, Dict

import emission.core.wrapper.entry as ecwe
import arrow 
import math


def generate_trip_coordinates(
    ref_coords: Tuple[float, float], 
    within_threshold: bool,
    threshold: float, 
    max: float = 0.1  # approx. 10km in WGS84 
    ) -> Tuple[float, float]:
    """generates trip coordinate data to use when mocking a set of trip data.

    :param origin: origin coordinates
    :param destination: destination coordinates
    :param trips: number of nearby coordinate pairs to generate
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
    :param mode_weights: sample weights, defaults to None, see random.choices "weights"
    :param purpose_weights: sample weights, defaults to None for uniform sampling
    :return: sampled trip labels
    """
    mw = [1.0 / len(mode_labels) for i in range(len(mode_labels))] \
        if mode_weights is not None else mode_weights
    rw = [1.0 / len(replaced_mode_labels) for i in range(len(replaced_mode_labels))] \
        if replaced_mode_weights is not None else replaced_mode_weights
    pw = [1.0 / len(purpose_labels) for i in range(len(purpose_labels))] \
        if purpose_weights is not None else purpose_weights
    mode_label_samples = random.choices(population=mode_labels, k=1, weights=mw)
    replaced_mode_label_samples = random.choices(population=replaced_mode_labels, k=1, weights=rw)
    purpose_label_samples = random.choices(population=purpose_labels, k=1, weights=pw)
    user_input = {
        "mode_confirm": mode_label_samples[0],
        "replaced_mode": replaced_mode_label_samples[0],
        "purpose_confirm": purpose_label_samples[0]
    }
    return user_input


def build_mock_trip(user_id, origin, destination, labels) -> Dict:
    key = "analysis/confirmed_trip"
    data = {
        "start_loc": {
            "coordinates": origin
        },
        "end_loc": {
            "coordinates": destination
        },
        "user_input": labels
    }

    return ecwe.Entry.create_fake_entry(user_id, key, data, write_ts=arrow.now())


def generate_mock_trips(
    user_id, 
    trips,
    origin, 
    destination, 
    label_data = None, 
    within_threshold = None,
    threshold = 0.01,
    max = 0.1, 
    has_label_p = 0.7,
    seed = 0):
    
    random.seed(seed)
    within = within_threshold if within_threshold is not None else trips
    trips_within_threshold = [i < within for i in range(trips)]
    result = []
    for within in trips_within_threshold:
        o = generate_trip_coordinates(origin, within, threshold, max)
        d = generate_trip_coordinates(destination, within, threshold, max)
        labels = {} if label_data is None or random.random() > has_label_p \
            else sample_trip_labels(
            mode_labels=label_data.get('mode_labels'),
            replaced_mode_labels=label_data.get('replaced_mode_labels'),
            purpose_labels=label_data.get('purpose_labels'),
            mode_weights=label_data.get('mode_weights'),
            replaced_mode_weights=label_data.get('replaced_mode_weights'),
            purpose_weights=label_data.get('purpose_weights')
        )
        trip = build_mock_trip(user_id, o, d, labels)
        result.append(trip)
        
    random.shuffle(result) 
    return result


if __name__ == '__main__':
    label_data = {
        "mode_labels": ['walk', 'bike', 'drive'],
        "purpose_labels": ['work', 'home', 'school'],
        "replaced_mode_labels": ['walk', 'bike', 'drive']
    }
    result = generate_mock_trips('joe-bob', 14, [0, 0], [1,1], label_data, 6)
    for r in result:
        print(r)