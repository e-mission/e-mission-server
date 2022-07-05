import pandas as pd
import numpy as np

# our imports
import emission.storage.decorations.trip_queries as esdtq
import emission.analysis.modelling.tour_model_first_only.build_save_model as bsm
import emission.analysis.modelling.tour_model_first_only.evaluation_pipeline as ep
from emission.analysis.classification.inference.labels.inferrers import predict_cluster_confidence_discounting

RADIUS = 500


class first_round_cluster():
    """ temporary class that implements first round clustering so that we don't have to run
        the whole pipeline. also adds fit and predict methods so that we can use this in our
        custom cross-validation function. 
    """

    def __init__(self, user_id, radius=RADIUS):
        self.user_id = user_id
        self.radius = radius

    def fit(self, train_trips):
        """ Args:
                train_trips: list of trips
        """
        # copied from bsm.build_user_model()

        sim, bins, bin_trips, train_trips = ep.first_round(
            train_trips, self.radius)

        # save all user labels
        bsm.save_models('user_labels',
                        bsm.create_user_input_map(train_trips, bins),
                        self.user_id)

        # save location features of all bins
        bsm.save_models('locations',
                        bsm.create_location_map(train_trips,
                                                bins), self.user_id)

    def predict(self, test_trips):
        """ Args:
                test_trips: list of trips
                
            Returns:
                tuple of lists: (mode_pred, purpose_pred, replaced_pred, confidence)
        """
        mode_pred = []
        purpose_pred = []
        replaced_pred = []
        confidence = []

        for trip in test_trips:
            predictions = predict_cluster_confidence_discounting(trip)

            if len(predictions) == 0:
                mode_pred.append(np.nan)
                purpose_pred.append(np.nan)
                replaced_pred.append(np.nan)
                confidence.append(0)

            else:
                predictions_df = pd.DataFrame(predictions).rename(
                    columns={'labels': 'user_input'})
                # renaming is simply so we can use the expand_userinputs
                # function

                expand_predictions = esdtq.expand_userinputs(predictions_df)
                # converts the 'labels' dictionaries into individual columns

                id_max = expand_predictions.p.idxmax()

                # sometimes we aren't able to predict all labels in the tuple,
                # so we have to handle that
                if 'mode_confirm' in expand_predictions.columns:
                    top_mode = expand_predictions.loc[id_max, 'mode_confirm']
                else:
                    top_mode = np.nan

                if 'purpose_confirm' in expand_predictions.columns:
                    top_purpose = expand_predictions.loc[id_max,
                                                         'purpose_confirm']
                else:
                    top_purpose = np.nan

                if 'replaced_mode' in expand_predictions.columns:
                    top_replaced = expand_predictions.loc[id_max,
                                                          'replaced_mode']
                else:
                    top_replaced = np.nan

                top_conf = expand_predictions.loc[id_max, 'p']

                mode_pred.append(top_mode)
                purpose_pred.append(top_purpose)
                replaced_pred.append(top_replaced)
                confidence.append(top_conf)

        return mode_pred, purpose_pred, replaced_pred, confidence
