from typing import List
import emission.core.wrapper.confirmedtrip as ecwc


def origin_features(trip: ecwc.Confirmedtrip) -> List[float]:
    """extract the trip origin coordinates.

    :param trip: trip to extract features from
    :return: origin coordinates
    """
    try:
        origin = trip['data']['start_loc']["coordinates"]
        return origin
    except KeyError as e:
        msg = 'Confirmedtrip expected to have path data.start_loc.coordinates'
        raise KeyError(msg) from e

def destination_features(trip: ecwc.Confirmedtrip) -> List[float]:
    """extract the trip destination coordinates. 

    :param trip: trip to extract features from
    :return: destination coordinates
    """
    try:
        destination = trip['data']['end_loc']["coordinates"]
        return destination
    except KeyError as e:
        msg = 'Confirmedtrip expected to have path data.start_loc.coordinates'
        raise KeyError(msg) from e
    

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
    try:
        return [trip['data']['distance']]
    except KeyError as e:
        msg = 'Confirmedtrip expected to have path data.distance'
        raise KeyError(msg) from e

def duration_feature(trip: ecwc.Confirmedtrip) -> List[float]:
    """provided for forward compatibility.

    :param trip: trip to extract features from
    :return: duration feature
    """
    try:
        return [trip['data']['duration']]
    except KeyError as e:
        msg = 'Confirmedtrip expected to have path data.duration'
        raise KeyError(msg) from e
