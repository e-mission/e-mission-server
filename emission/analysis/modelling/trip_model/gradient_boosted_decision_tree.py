import logging
from tokenize import group
from typing import Dict, List, Optional, Tuple

import sklearn.ensemble as ske

import emission.analysis.modelling.trip_model.trip_model as eamuu
import emission.analysis.modelling.trip_model.util as eamtu
import emission.analysis.modelling.trip_model.config as eamtc
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
        choice wasn't available). These labels are gathered from the user 
        along with the chosen mode and trip purpose after the trip takes place.

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
        self.gbdt = ske.GradientBoostingClassifier(n_estimators=50)
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

    def predict(self, trip: ecwc.Confirmedtrip) -> List[str]:
        logging.debug(f"running gradient boosted mode prediction")
        X_test, y_pred = self.extract_features(trip, is_prediction=True)
        y_pred = self.gbdt.predict(X_test)
        if y_pred is None:
            logging.debug(f"unable to predict mode for trip {trip}")
            return []
        else:
            logging.debug(f"made predictions {y_pred}")
            return y_pred

    def to_dict(self) -> Dict:
        return self.gbdt.get_params()

    def from_dict(self, model: Dict):
        self.gbdt.set_params(model)

    def extract_features(self, trips: ecwc.Confirmedtrip, is_prediction=False) -> List[float]:
        return eamtu.get_replacement_mode_features(self.feature_list, self.dependent_var, trips, is_prediction)
