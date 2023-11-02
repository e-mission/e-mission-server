from typing import List, Tuple
from past.utils import old_div
import numpy as np
import pandas as pd
from numpy.linalg import norm

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
    a = np.array([x[0], values[0]])
    b = np.array([x[-1], values[-1]])
    n = norm(b - a)
    new_y = []
    for i in range(0, N):
        p = np.array([x[i], values[i]])
        dist = old_div(norm(np.cross(p - a, p - b)), n)
        new_y.append(dist)
        if dist > max:
            max = dist
            index = i
    value = values[index]
    return [index, value]

    def get_distance_matrix(loc_df, loc_type):
        """ Args:
                loc_df (dataframe): must have columns 'start_lat' and 'start_lon' 
                    or 'end_lat' and 'end_lon'
                loc_type (str): 'start' or 'end'
        """
        assert loc_type == 'start' or loc_type == 'end'

        radians_lat_lon = np.radians(loc_df[[loc_type + "_lat", loc_type + "_lon"]])

        dist_matrix_meters = pd.DataFrame(
            smp.haversine_distances(radians_lat_lon, radians_lat_lon) *
            EARTH_RADIUS)
        return dist_matrix_meters

def single_cluster_purity(points_in_cluster, label_col='purpose_confirm'):
    """ Calculates purity of a cluster (i.e. % of trips that have the most 
        common label)
    
        Args:
            points_in_cluster (df): dataframe containing points in the same 
                cluster
            label_col (str): column in the dataframe containing labels
    """
    assert label_col in points_in_cluster.columns

    most_freq_label = points_in_cluster[label_col].mode()[0]
    purity = len(points_in_cluster[points_in_cluster[label_col] ==
                                   most_freq_label]) / len(points_in_cluster)
    return purity
