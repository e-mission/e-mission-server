from typing import List, Tuple
from past.utils import old_div
import numpy as np
import pandas as pd
from numpy.linalg import norm
import copy

from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import make_pipeline
from sklearn.impute import SimpleImputer


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


class OneHotWrapper():
    """ Helper class to streamline one-hot encoding. 
    
        Args: 
            impute_missing (bool): whether or not to impute np.nan values. 
            sparse (bool): whether or not to return a sparse matrix. 
            handle_unknown (str): specifies the way unknown categories are 
                handled during transform.
    """

    def __init__(
        self,
        impute_missing=False,
        sparse=False,
        handle_unknown='ignore',
    ):
        self.impute_missing = impute_missing
        if self.impute_missing:
            self.encoder = make_pipeline(
                SimpleImputer(missing_values=np.nan,
                              strategy='constant',
                              fill_value='missing'),
                OneHotEncoder(sparse=False, handle_unknown=handle_unknown))
        else:
            self.encoder = OneHotEncoder(sparse=sparse,
                                         handle_unknown=handle_unknown)

    def fit_transform(self, train_df, output_col_prefix=None):
        """ Establish one-hot encoded variables. 
        
            Args: 
                train_df (DataFrame): DataFrame containing train trips. 
                output_col_prefix (str): only if train_df is a single column
        """
        # TODO: handle pd series

        train_df = train_df.copy()  # to avoid SettingWithCopyWarning

        # if imputing, the dtype of each column must be string/object and not
        # numerical, otherwise the SimpleImputer will fail
        if self.impute_missing:
            for col in train_df.columns:
                train_df[col] = train_df[col].astype(object)
        onehot_encoding = self.encoder.fit_transform(train_df)
        self.onehot_encoding_cols_all = []
        for col in train_df.columns:
            if train_df.shape[1] > 1 or output_col_prefix is None:
                output_col_prefix = col
            self.onehot_encoding_cols_all += [
                f'{output_col_prefix}_{val}'
                for val in np.sort(train_df[col].dropna().unique())
            ]
            # we handle np.nan separately because it is of type float, and may
            # cause issues with np.sort if the rest of the unique values are
            # strings
            if any((train_df[col].isna())):
                self.onehot_encoding_cols_all += [f'{output_col_prefix}_nan']

        onehot_encoding_df = pd.DataFrame(
            onehot_encoding,
            columns=self.onehot_encoding_cols_all).set_index(train_df.index)

        # ignore the encoded columns for missing entries
        self.onehot_encoding_cols = copy.deepcopy(self.onehot_encoding_cols_all)
        for col in self.onehot_encoding_cols_all:
            if col.endswith('_nan'):
                onehot_encoding_df = onehot_encoding_df.drop(columns=[col])
                self.onehot_encoding_cols.remove(col)

        return onehot_encoding_df.astype(int)

    def transform(self, test_df):
        """ One-hot encoded features in accordance with features seen in the 
            train set. 
        
            Args: 
                test_df (DataFrame): DataFrame of trips. 
        """
        # TODO: rename test_df, this one doesn't necessarily need to be a df
        onehot_encoding = self.encoder.transform(test_df)
        onehot_encoding_df = pd.DataFrame(
            onehot_encoding,
            columns=self.onehot_encoding_cols_all).set_index(test_df.index)

        # ignore the encoded columns for missing entries
        for col in self.onehot_encoding_cols_all:
            if col.endswith('_nan'):
                onehot_encoding_df = onehot_encoding_df.drop(columns=[col])

        return onehot_encoding_df.astype(int)