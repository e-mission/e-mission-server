from typing import List
import emission.core.wrapper.confirmedtrip as ecwc
import emission.analysis.modelling.tour_model.label_processing as lp


def origin_features(trip: ecwc.Confirmedtrip) -> List[float]:
    """extract the trip origin coordinates.

    :param trip: trip to extract features from
    :return: origin coordinates
    """
    origin = trip.data.start_loc["coordinates"]
    return origin

def destination_features(trip: ecwc.Confirmedtrip) -> List[float]:
    """extract the trip destination coordinates. 

    :param trip: trip to extract features from
    :return: destination coordinates
    """
    destination = trip.data.end_loc["coordinates"]
    return destination

def od_features(trip: ecwc.Confirmedtrip) -> List[float]:
    """extract both origin and destination coordinates.

    :param trip: trip to extract features from
    :return: od coordinates
    """
    o_lat, o_lon = origin_features(trip)
    d_lat, d_lon = destination_features(trip)
    return [o_lat, o_lon, d_lat, d_lon]

def distance_feature(trip: ecwc.Confirmedtrip) -> List[float]:
    """provided for forward compatibility.

    :param trip: trip to extract features from
    :return: distance feature
    """
    return [trip.data.distance]

def duration_feature(trip: ecwc.Confirmedtrip) -> List[float]:
    """provided for forward compatibility.

    :param trip: trip to extract features from
    :return: duration feature
    """
    return [trip.data.duration]
