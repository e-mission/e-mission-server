from typing import Dict, List
from emission.core.wrapper.confirmedtrip import Confirmedtrip
import emission.analysis.modelling.tour_model.label_processing as lp


def origin_features(trip: Confirmedtrip) -> List[float]:
    origin = trip.data.start_loc["coordinates"]
    return origin

def destination_features(trip: Confirmedtrip) -> List[float]:
    destination = trip.data.end_loc["coordinates"]
    return destination

def od_features(trip: Confirmedtrip) -> List[float]:
    o_lat, o_lon = origin_features(trip)
    d_lat, d_lon = destination_features(trip)
    return [o_lat, o_lon, d_lat, d_lon]

def distance_feature(trip: Confirmedtrip) -> List[float]:
    return [trip.data.distance]

def duration_feature(trip: Confirmedtrip) -> List[float]:
    return [trip.data.duration]

def label_features(trip: Confirmedtrip) -> Dict:
    labels = trip.data.user_input
    labels_normalized = lp.map_labels(labels)  # could be replaced by localization logic
    return labels_normalized
