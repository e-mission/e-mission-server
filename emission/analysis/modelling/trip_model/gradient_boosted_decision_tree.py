import logging
from tokenize import group
from typing import Dict, List, Optional, Tuple

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
import emission.core.wrapper.confirmedtrip as ecwc


class GradientBoostedDecisionTree(eamuu.TripModel):

    is_incremental: bool = False  # overwritten during __init__
    class_map: dict = {}  # overwritten during fit

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
        # Use the sklearn implementation of a GBDT
        self.gbdt = GradientBoostingClassifier(n_estimators=50)
        # Which features to use in the fit/prediction
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

    def predict(self, trip: ecwc.Confirmedtrip) -> Tuple[List[Dict], int]:
        logging.debug(f"running gradient boosted mode prediction")
        X_train, y_train = self.extract_features(trip)
        y_pred = self.gbdt.predict(X_train)
        if y_pred is None:
            logging.debug(f"unable to predict bin for trip {trip}")
            return []
        else:
            logging.debug(f"made predictions {y_pred}")
            return y_pred

    def to_dict(self) -> Dict:
        return self.gbdt

    def from_dict(self, model: Dict):
        self.gbdt = model

    def extract_features(self, trips: ecwc.Confirmedtrip) -> List[float]:
        # TODO: need to enable generic paths other than just user input for features
        X = pd.DataFrame(
            [[trip['data']['user_input'][x] for x in self.feature_list] for trip in trips],
            columns=self.feature_list
        )
        y = pd.DataFrame(
            [trip['data']['user_input'][self.dependent_var] for trip in trips],
            columns=[self.dependent_var]
        )
        # Clean up and recode the feature columns for training/prediction
        X_processed, y_processed = self._process_data(X, y)
        return X_processed, y_processed

    def _process_data(self, X, y):
        """
        helper function to transform binned features and labels.
        """
        # Any non-numeric dtype must be one-hot encoded (if unordered) or numerically coded (if ordered)
        dummies = []
        for col in X:
            if X[col].dtype=='object':
                dummies.append(pd.get_dummies(X[col], prefix=col))
        X = pd.concat(dummies, axis=1)
        # The outcome must be a single categorical column; recode to numeric
        for col in y:
            cat_list = list(pd.unique(y[col])).sort()
            if y[col].dtype=='object':
                y[col] = pd.Categorical(y[col], ordered=True, categories=cat_list)
                y[col] = y[col].cat.codes
        return X, y
