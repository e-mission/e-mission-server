import logging
from tokenize import group
from typing import Dict, List, Optional, Tuple

import numpy as np
import sklearn as ske

import emission.analysis.modelling.trip_model.trip_model as eamuu
import emission.analysis.modelling.trip_model.config as eamtc
import emission.core.wrapper.confirmedtrip as ecwc
import emission.analysis.modelling.trip_model.util as eamtu


class SupportVectorMachine(eamuu.TripModel):

    is_incremental: bool = False  # overwritten during __init__
    is_initialized: bool = False  # overwritten during first fit()

    def __init__(self, config=None):
        """
        Instantiate a linear support vector machine for all users.

        This uses the sklearn implementation of a support vector machine 
        to classify unlabeled replacement modes. The SVM is linear, and is fit 
        with the more general SGDClassifier class which can accommodate online 
        learning:

        https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.SGDClassifier.html

        For anyone looking to implement a differnt online learning model in the 
        future, here is a list of sklearn models that implement "partial_fit" 
        and would be candidates for the online learning approach implemented 
        here:

        https://scikit-learn.org/0.15/modules/scaling_strategies.html

        Replacement modes are considered to be the second-best choice for
        a given trip (i.e., what mode would have been chosen if the actual
        choice wasn't available). These labels are gathered from the user 
        along with the chosen mode and trip purpose after the trip takes place.

        The model is currently trained on data from all users.
        """
        if config is None:
            config = eamtc.get_config_value_or_raise('model_parameters.svm')
            logging.debug(f'SupportVectorMachine loaded model config from file')
        else:
            logging.debug(f'SupportVectorMachine using model config argument')
        expected_keys = [
            'incremental_evaluation',
            'feature_list',
            'dependent_var'
        ]
        for k in expected_keys:
            if config.get(k) is None:
                msg = f"svm trip model config missing expected key {k}"
                raise KeyError(msg)
        self.is_incremental = config['incremental_evaluation']
        # use the sklearn implementation of a svm
        self.svm = ske.linear_model.SGDClassifier()
        self.feature_list = config['feature_list']
        self.dependent_var = config['dependent_var']

    def fit(self, trips: List[ecwc.Confirmedtrip]):
        """train the model by passing data, where each row in the data
        corresponds to a label at the matching index of the label input.

        If using an incremental model, the initial call to fit will store 
        the list of unique classes in y. The config file is used to store 
        a lookup for known classes for each categorical feature. This prevents 
        the need to store a lookup in the model itself, which must be updated 
        every time the model sees a new class or feature OR when it is given 
        an incremental training request that does not contain every feature class
        etc.

        :param trips: 2D array of features to train from
        """
        logging.debug(f'fit called with {len(trips)} trips')
        unlabeled = list(filter(lambda t: len(t['data']['user_input']) == 0, trips))
        if len(unlabeled) > 0:
            msg = f'model.fit cannot be called with unlabeled trips, found {len(unlabeled)}'
            raise Exception(msg)
        X_train, y_train = self.extract_features(trips)
        # the first time partial_fit is called, the incremental classes are initialized to the unique y values
        if self.is_incremental and not self.is_initialized:
            logging.debug(f'initializing incremental model fit')
            self.svm.partial_fit(X_train, y_train, self.dependent_var['classes'])
            self.is_initialized = True
        # for all future partial fits, there is no need to pass the classes again
        elif self.is_incremental and self.is_initialized:
            logging.debug(f'updating incremental model fit')
            try:
                self.svm.partial_fit(X_train, y_train)
            except ValueError:
                raise ValueError("Error in incremental fit: Likely an unseen feature or dependent class was found")
        # if not incremental, just train regularly
        else:
            self.svm.fit(X_train, y_train)
        logging.info(f"support vector machine model fit to {len(X_train)} rows of trip data")
        logging.info(f"training features were {X_train.columns}")

    def predict(self, trip: ecwc.Confirmedtrip) -> List[str]:
        logging.debug(f"running support vector mode prediction")
        X_test, y_pred = self.extract_features(trip, is_prediction=True)
        y_pred = self.svm.predict(X_test)
        if y_pred is None:
            logging.debug(f"unable to predict mode for trip {trip}")
            return []
        else:
            logging.debug(f"made predictions {y_pred}")
            return y_pred

    def to_dict(self) -> Dict:
        return self.svm.get_params()

    def from_dict(self, model: Dict):
        self.svm.set_params(model)

    def extract_features(self, trips: ecwc.Confirmedtrip, is_prediction=False) -> List[float]:
        return eamtu.get_replacement_mode_features(self.feature_list, self.dependent_var, trips, is_prediction)
