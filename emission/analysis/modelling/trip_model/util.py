from typing import List, Tuple
from past.utils import old_div
from math import radians, cos, sin, asin, sqrt
import numpy
from numpy.linalg import norm
import pandas as pd

import emission.core.get_database as edb
import emission.core.wrapper.confirmedtrip as ecwc


# TODO: Currently fails to match the uuid_list to what is in the database. Needs to be split into 1 function which reads demographic data, 1 function which formats features
def get_survey_df(uuid_list, survey_features):
    # we use the "survey." identifier to separate out the features which require survey attention in the config, but do not need it in actual key
    survey_features_response = [".".join(x.split(".")[1:]) for x in survey_features]
    # retrieve survey response records for any user that has supplied a trip which is being trained on
    all_survey_results = list(edb.get_timeseries_db().find({"user_id": {"$in": uuid_list}, "metadata.key": "manual/demographic_survey"}))
    # these are the uuids that were able to be retrieved from the database; if they don't match the requested ones throw an error since we cannot train/test
    survey_result_uuids = [s["user_id"] for s in all_survey_results]
    # the challenge is that one of the first dictionary keys changes across users, so we cannot apply a single json_normalize and take feature values
    # each unique key will end up as its own column, even if it is really the same feature as others, and will be NaN for all other users
    # this is the id that changes across users, we keep a list here to index later when summarizing all of the survey response results to a single df
    survey_response_ids = [list(s['data']['jsonDocResponse'].keys())[0] for s in all_survey_results]
    result = []
    for i, s in enumerate(all_survey_results):
        response = pd.json_normalize(s['data']['jsonDocResponse'][survey_response_ids[i]])
        response = response[survey_features_response]
        response.columns = survey_features
        response['user_id'] = survey_result_uuids[i]
        result.append(response)
    return pd.concat(result, axis=0).reset_index(inplace=True, drop=True)


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
        if 'survey' in x:
            survey_features.append(x)
        else:
            nonsurvey_features.append(x)
    # make sure no features are being lost during separation
    assert(len(survey_features) + len(nonsurvey_features) == len(feature_names))
    # need unique response id for every trip to identify survey features in the trip dataframe (key below jsonDocResponse)
    if len(survey_features) > 0:
        uuid_list = [list(trip['user_id'] for trip in trips)]
        survey_df = get_survey_df(uuid_list, survey_features)
        X = pd.concat([trips_df[nonsurvey_features], survey_df], axis=1)
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
