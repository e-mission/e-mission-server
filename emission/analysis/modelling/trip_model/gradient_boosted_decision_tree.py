import logging
from tokenize import group
from typing import Dict, List, Optional, Tuple

from math import radians, cos, sin, asin, sqrt
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
import sklearn.metrics as sm

import emission.storage.timeseries.abstract_timeseries as esta
import emission.analysis.modelling.tour_model.label_processing as lp
import emission.analysis.modelling.trip_model.trip_model as eamuu
import emission.analysis.modelling.trip_model.util as util
import emission.analysis.modelling.trip_model.config as eamtc
import emission.core.get_database as edb
import emission.core.wrapper.confirmedtrip as ecwc


class GradientBoostedDecisionTree(eamuu.TripModel):

    is_incremental: bool = False  # overwritten during __init__

    def __init__(self, config=None):
        """
        Instantiate a gradient boosted decision tree for all users.

        This uses the sklearn implementation of a gradient boosted
        decision tree to classify unlabeled replacement modes.
        
        https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.GradientBoostingClassifier.html

        Replacement modes are considered to be the second-best choice for
        a given trip (i.e., what mode would have been chosen if the actual
        choice wasn't available).

        The model is currently trained on data from all users.
        """
        if config is None:
            config = eamtc.get_config_value_or_raise('model_parameters.gbdt')
            logging.debug(f'GradientBoostedDecisionTree loaded model config from file')
        else:
            logging.debug(f'GradientBoostedDecisionTree using model config argument')
        expected_keys = [
            'incremental_evaluation',
            'feature_list',
            'dependent_var'
        ]
        for k in expected_keys:
            if config.get(k) is None:
                msg = f"gbdt trip model config missing expected key {k}"
                raise KeyError(msg)
        self.is_incremental = config['incremental_evaluation']
        # use the sklearn implementation of a GBDT
        self.gbdt = GradientBoostingClassifier(n_estimators=50)
        self.feature_list = config['feature_list']
        self.dependent_var = config['dependent_var']

    def fit(self, trips: List[ecwc.Confirmedtrip]):
        """train the model by passing data, where each row in the data
        corresponds to a label at the matching index of the label input

        :param trips: 2D array of features to train from
        """
        logging.debug(f'fit called with {len(trips)} trips')
        unlabeled = list(filter(lambda t: len(t['data']['user_input']) == 0, trips))
        if len(unlabeled) > 0:
            msg = f'model.fit cannot be called with unlabeled trips, found {len(unlabeled)}'
            raise Exception(msg)
        X_train, y_train = self.extract_features(trips)
        self.gbdt.fit(X_train, y_train)
        logging.info(f"gradient boosted decision tree model fit to {len(X_train)} rows of trip data")
        logging.info(f"training features were {X_train.columns}")

    def predict(self, trip: ecwc.Confirmedtrip) -> List[int]:
        logging.debug(f"running gradient boosted mode prediction")
        X_test, y_pred = self.extract_features(trip, is_prediction=True)
        y_pred = self.gbdt.predict(X_test)
        if y_pred is None:
            logging.debug(f"unable to predict bin for trip {trip}")
            return []
        else:
            logging.debug(f"made predictions {y_pred}")
            return y_pred

    def to_dict(self) -> Dict:
        return self.gbdt.get_params()

    def from_dict(self, model: Dict):
        self.gbdt.set_params(model)

    def extract_features(self, trips: ecwc.Confirmedtrip, is_prediction=False) -> List[float]:
        # get dataframe from json trips; fill in calculated columns
        trips_df = pd.json_normalize(trips)
        # distance
        trips_coords = trips_df[['data.start_loc.coordinates','data.end_loc.coordinates']]
        trips_df['distance_miles'] = trips_coords.apply(lambda row : self.haversine(row[0],row[1]), axis=1)
        # collect all features
        X = trips_df[self.feature_list]
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
            y = trips_df[self.dependent_var].values
        return X, y

    # if the non-mock trips have distance calculated then this can be removed
    # https://stackoverflow.com/questions/4913349/haversine-formula-in-python-bearing-and-distance-between-two-gps-points
    def haversine(self, coord1, coord2):
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
