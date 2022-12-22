from typing import List, Tuple
from past.utils import old_div
from math import radians, cos, sin, asin, sqrt
import numpy
from numpy.linalg import norm
import pandas as pd

import emission.core.wrapper.confirmedtrip as ecwc


def get_survey_df(trips_df, survey_features, response_ids):
    survey_data = []
    for feature in survey_features:
        feature_data = []
        for i, response_id in enumerate(response_ids):
            feature_string = feature.split(".")
            feature_string.insert(2, response_id)
            feature_string = ".".join(feature_string)
            feature_data.append(trips_df.iloc[i,][feature_string])
        survey_data.append(feature_data)
    return pd.DataFrame(numpy.column_stack(survey_data), columns=survey_features)


def get_replacement_mode_features(feature_list, dependent_var, trips: ecwc.Confirmedtrip, is_prediction=False) -> List[float]:
    """extract the features needed to perform replacement mode modeling from a set of
    trips.

    recodes variables that are categorical.

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
    # get dataframe from json trips
    trips_df = pd.json_normalize(trips)
    # any features that are part of the demographic survey require special attention
    # the first nested value of the survey data responses changes depending on the user/response
    feature_names = list(feature_list.keys())
    survey_features = []
    nonsurvey_features = []
    for x in feature_names:
        if 'jsonDocResponse' in x:
            survey_features.append(x)
        else:
            nonsurvey_features.append(x)
    # make sure no features are being lost during separation
    assert(len(survey_features) + len(nonsurvey_features) == len(feature_names))
    # need unique response id for every trip to identify survey features in the trip dataframe (key below jsonDocResponse)
    if len(survey_features) > 0:
        response_ids = [list(trip['data']['jsonDocResponse'].keys())[0] for trip in trips]
        X = pd.concat([trips_df[nonsurvey_features], get_survey_df(trips_df, survey_features, response_ids)], axis=1)
    else:
        X = trips_df[nonsurvey_features]
    # any features that are strings must be encoded as numeric variables
    # we use one-hot encoding for categorical variables
    # https://pbpython.com/pandas_dtypes.html
    dummies = []
    numeric = []
    for col in X:
        # object column == string or mixed variable
        if X[col].dtype=='object':
            cat_col = pd.Categorical(X[col], categories=feature_list[col])
            # if new features are present in X_test, throw value error
            if cat_col.isnull().any():
                raise ValueError(f"Cannot predict on unseen classes in: {col}")
            dummies.append(pd.get_dummies(cat_col, prefix=col))
        else:
            numeric.append(X[col])
    numeric.extend(dummies)
    X = pd.concat(numeric, axis=1)
    # only extract dependent var if fitting a new model
    # for the dependent variable of a classification model, sklearn will accept strings
    # so no need to recode these to numeric and deal with complications of storing labels
    # and decoding them later
    if is_prediction:
        y = None
    else:
        y = trips_df[dependent_var['name']]
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
