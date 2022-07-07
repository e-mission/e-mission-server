import pandas as pd
import numpy as np

import sklearn.cluster as sc

# our imports
import clustering
import data_wrangling
import emission.storage.decorations.trip_queries as esdtq
import emission.analysis.modelling.tour_model_first_only.build_save_model as bsm
import emission.analysis.modelling.tour_model_first_only.evaluation_pipeline as ep
from emission.analysis.classification.inference.labels.inferrers import predict_cluster_confidence_discounting

RADIUS = 500


class old_clustering():
    """ temporary class that implements first round clustering so that we don't 
        have to run the whole pipeline. also adds fit and predict methods so 
        that we can use this in our custom cross-validation function. 

        NOTE: the output is not up to date (this only has tuple-level 
        confidence; does not contain separated confidences for each label 
        category)
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


class new_clustering():
    """ quick and dirty code that implements our new clustering so that we can 
        get some predictions out the door. also adds fit and predict methods so 
        that we can use this in our custom cross-validation function. 

        NOTE: this code is extremely raw 
        - need to update before it can be used in cross validation (distinguish 
        between train and test trips). 
        - Hyperparameters have been chosen pretty arbitrarily. Will do more 
        tuning later. 
        - This is basically copied from the clustering.py code. I should 
        refactor that to call this instead.
    """

    def __init__(
            self,
            # user_id,
            trips_df,
            radius=150):
        """ Args:
                trips_df (dataframe): dataframe of trips to train on. If None, will use all the user's trips (both labeled and unlabeled) to create clusters. Only labeled trips are used in the SVM sub-division process. 
        """
        # self.user_id = user_id
        self.radius = radius
        self.trips_df = trips_df

        # clean up the dataframe by dropping entries with NaN locations and
        # reset index
        self.trips_df = self.trips_df.dropna(
            subset=['start_loc', 'end_loc']).reset_index(drop=True)

        # expand the 'start_loc' and 'end_loc' column into 'start_lat',
        # 'start_lon', 'end_lat', and 'end_lon' columns
        self.trips_df = data_wrangling.expand_coords(self.trips_df)

        # expand the 'user_input' columns into 'mode_confirm',
        # 'purpose_confirm', and 'replaced_mode' if it hasn't been done so yet
        if 'purpose_confirm' not in self.trips_df.columns:
            self.trips_df = esdtq.expand_userinputs(self.trips_df)

    def fit(self):
        """ assigns every trip to a cluster. self.trips_df will be updated with 
            a new column, 'end_mean_shift_clusters_{self.radius}_m, which 
            contains the cluster indices. 
        """
        labels = sc.MeanShift(bandwidth=0.000005 * self.radius,
                              # min_bin_freq=2,
                              ).fit(self.trips_df[["end_lon",
                                                   "end_lat"]]).labels_

        # pd.Categorical converts the type from int to category (so
        # numerical operations aren't possible)
        # loc_df.loc[:,
        #            f"{loc_type}_DBSCAN_clusters_{r}_m"] = pd.Categorical(
        #                labels)
        # TODO: fix this and make it Categorical again (right now labels are
        # ints)
        self.trips_df.loc[:, 'cluster_idx'] = labels

        # move "noisy" trips to their own single-trip clusters
        for idx in self.trips_df.loc[self.trips_df['cluster_idx'] ==
                                     -1].index.values:
            self.trips_df.loc[
                idx, 'cluster_idx'] = 1 + self.trips_df['cluster_idx'].max()

        self.trips_df = clustering.add_loc_SVM(self.trips_df,
                                               radii=[self.radius],
                                               alg='mean_shift',
                                               loc_type='end',
                                               svm_min_size=6,
                                               svm_purity_thresh=0.7,
                                               svm_gamma=0.05,
                                               svm_C=1,
                                               cluster_cols=['cluster_idx'])

    def predict(self):
        """ Generate predictions for all unlabeled trips (if possible). adds 3 
            new columns to self.trips_df: 'mode_pred', 'purpose_pred', 
            'replaced_pred'. The entries of these columns are dictionaries, 
            where the keys are the predicted labels and the values are the 
            associated probabilities/confidences. 
        """

        # right now, everything with the same end cluster will have the same
        # prediction (since we're relying solely on distribution of existing
        # labels in the cluster.) thus, we can simplify the process by
        # assigning labels to all trips in a cluster at once. this is super raw
        # and should not be used for cross-validation
        self.trips_df.loc[:, ['mode_pred', 'purpose_pred', 'replaced_pred'
                              ]] = np.nan

        for c in self.trips_df.loc[:, 'cluster_idx'].unique():
            trips_in_cluster = self.trips_df.loc[self.trips_df.cluster_idx ==
                                                 c]

            # get distribution of labels in this cluster
            mode_distrib = trips_in_cluster.mode_confirm.value_counts(
                normalize=True, dropna=True).to_dict()
            purpose_distrib = trips_in_cluster.purpose_confirm.value_counts(
                normalize=True, dropna=True).to_dict()
            replaced_distrib = trips_in_cluster.replaced_mode.value_counts(
                normalize=True, dropna=True).to_dict()

            # TODO: add confidence discounting

            # update predictions
            # convert the dict into a list of dicts to work around pandas
            # thinking we're trying to insert information according to a
            # key-value map or something
            cluster_size = len(
                self.trips_df.loc[self.trips_df.cluster_idx == c])
            self.trips_df.loc[self.trips_df.cluster_idx == c,
                              'mode_pred'] = [mode_distrib] * cluster_size
            self.trips_df.loc[self.trips_df.cluster_idx == c,
                              'purpose_pred'] = [purpose_distrib
                                                 ] * cluster_size
            self.trips_df.loc[self.trips_df.cluster_idx == c,
                              'replaced_pred'] = [replaced_distrib
                                                  ] * cluster_size
