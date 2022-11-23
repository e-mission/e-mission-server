from typing import List, Tuple
from past.utils import old_div
from math import radians, cos, sin, asin, sqrt
import numpy
from numpy.linalg import norm
import pandas as pd

import emission.core.wrapper.confirmedtrip as ecwc


def get_replacement_mode_features(feature_list, dependent_var, is_prediction, trips: ecwc.Confirmedtrip) -> List[float]:
    """extract the features needed to perform replacement mode modeling from a set of
    trips.

    recodes variables that are categorical, and (TODO: scales numeric variables 0-1).

    :param feature_list: features to gather from each trip
    :type feature_list: List[string]
    :param dependent_var: the feature to predict for each trip
    :type dependent_var: string
    :param is_prediction: whether or not to extract the dependent var
    :type is_prediction: bool
    :param trips: all trips to extract features from
    :type trips: List[ecwc.Confirmedtrip]
    :return: the training X features and y for the replacement mode model
    :rtype: Tuple[List[List[float]], List[]]
    """
    # get dataframe from json trips; fill in calculated columns
    trips_df = pd.json_normalize(trips)
    # distance
    trips_coords = trips_df[['data.start_loc.coordinates','data.end_loc.coordinates']]
    trips_df['distance_miles'] = trips_coords.apply(lambda row : haversine(row[0],row[1]), axis=1)
    # collect all features
    X = trips_df[feature_list]
    # any object/categorical dtype features must be one-hot encoded if unordered
    dummies = []
    for col in X:
        if X[col].dtype=='object':
            dummies.append(pd.get_dummies(X[col], prefix=col))
    X = pd.concat(dummies, axis=1)
    # Only extract dependent var if fitting a new model
    if is_prediction:
        y = None
    else:
        y = trips_df[dependent_var].values
    return X, y


def find_knee_point(values: List[float]) -> Tuple[float, int]:
    """for a list of values, find the value which represents the cut-off point
    or "elbow" in the function when values are sorted.

    copied from original similarity algorithm. permalink:
    [https://github.com/e-mission/e-mission-server/blob/5b9e608154de15e32df4f70a07a5b95477e7dbf5/emission/analysis/modelling/tour_model/similarity.py#L256]

    with `y` passed in as `values`
    based on this stack overflow answer: https://stackoverflow.com/a/2022348/4803266
    And summarized by the statement: "A quick way of finding the elbow is to draw a
    line from the first to the last point of the curve and then find the data point
    that is farthest away from that line."

    :param values: list of values from which to select a cut-off
    :type values: List[float]
    :return: the index and value to use as a cutoff
    :rtype: Tuple[int, float]
    """
    N = len(values)
    x = list(range(N))
    max = 0
    index = -1
    a = numpy.array([x[0], values[0]])
    b = numpy.array([x[-1], values[-1]])
    n = norm(b - a)
    new_y = []
    for i in range(0, N):
        p = numpy.array([x[i], values[i]])
        dist = old_div(norm(numpy.cross(p - a, p - b)), n)
        new_y.append(dist)
        if dist > max:
            max = dist
            index = i
    value = values[index]
    return [index, value]


# if the non-mock trips have distance calculated then this can be removed
# https://stackoverflow.com/questions/4913349/haversine-formula-in-python-bearing-and-distance-between-two-gps-points
def haversine(coord1, coord2):
    """
    Calculate the great circle distance in kilometers between two points 
    on the earth (specified in decimal degrees)
    """
    lon1 = coord1[0]
    lat1 = coord1[1]
    lon2 = coord2[0]
    lat2 = coord2[1]
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 3956 # radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return c * r