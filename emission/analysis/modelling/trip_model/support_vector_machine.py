import logging
from tokenize import group
from typing import Dict, List, Optional, Tuple

import numpy as np
from sklearn.linear_model import SGDClassifier

import emission.analysis.modelling.trip_model.trip_model as eamuu
import emission.analysis.modelling.trip_model.config as eamtc
import emission.core.wrapper.confirmedtrip as ecwc
import emission.analysis.modelling.trip_model.util as eamtu


class SupportVectorMachine(eamuu.TripModel):

    is_incremental: bool = False  # overwritten during __init__
    incremental_classes: list = None # overwritten when fit is initialized

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
        choice wasn't available).

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
        self.svm = SGDClassifier()
        self.feature_list = config['feature_list']
        self.dependent_var = config['dependent_var']

    def fit(self, trips: List[ecwc.Confirmedtrip]):
        """train the model by passing data, where each row in the data
        corresponds to a label at the matching index of the label input.

        If using an incremental model, the initial call to fit will store 
        the list of unique classes in y. If additional classes are added in the 
        future, the model will need to be re-initialized on data that 
        contains all of those classes, otherwise sklearn will throw an 
        error. Additionally, any categorical variables used as features 
        must have all classes present in the initial training data. If not, 
        they will throw an sklearn error due to differences in the number of 
        OHE variables in the data.

        :param trips: 2D array of features to train from
        """
        logging.debug(f'fit called with {len(trips)} trips')
        unlabeled = list(filter(lambda t: len(t['data']['user_input']) == 0, trips))
        if len(unlabeled) > 0:
            msg = f'model.fit cannot be called with unlabeled trips, found {len(unlabeled)}'
            raise Exception(msg)
        X_train, y_train = self.extract_features(trips)
        # The first time partial_fit is called, it must include a list of the unique classes in y
        if self.is_incremental and self.incremental_classes is None:
            logging.debug(f'initializing incremental model fit')
            self.incremental_classes = np.unique(y_train)
            self.svm.partial_fit(X_train, y_train, self.incremental_classes)
        # For all future partial fits, there is no need to pass the classes again
        elif self.is_incremental and self.incremental_classes is not None:
            logging.debug(f'updating incremental model fit')
            self.svm.partial_fit(X_train, y_train)
        # If not incremental, train regularly
        else:
            self.svm.fit(X_train, y_train)
        logging.info(f"support vector machine model fit to {len(X_train)} rows of trip data")
        logging.info(f"training features were {X_train.columns}")

    def predict(self, trip: ecwc.Confirmedtrip) -> List[int]:
        logging.debug(f"running support vector mode prediction")
        X_test, y_pred = self.extract_features(trip, is_prediction=True)
        y_pred = self.svm.predict(X_test)
        if y_pred is None:
            logging.debug(f"unable to predict bin for trip {trip}")
            return []
        else:
            logging.debug(f"made predictions {y_pred}")
            return y_pred

    def to_dict(self) -> Dict:
        return self.svm.get_params()

    def from_dict(self, model: Dict):
        self.svm.set_params(model)

    def extract_features(self, trips: ecwc.Confirmedtrip, is_prediction=False) -> List[float]:
        return eamtu.get_replacement_mode_features(self.feature_list, self.dependent_var, is_prediction, trips)
