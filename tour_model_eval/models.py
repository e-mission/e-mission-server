import pandas as pd
import numpy as np
import logging

from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
import sklearn.metrics.pairwise as smp
from sklearn.cluster import DBSCAN
from sklearn import svm
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier, GradientBoostingClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.exceptions import NotFittedError

# hack because jupyter notebook doesn't work properly through my vscode for
# some reason and therefore cant import stuff from emission? remove this before
# pushing
###
import sys

sys.path.append('/Users/hlu2/Documents/GitHub/e-mission-server/')
###

# our imports
import clustering
import data_wrangling
import emission.storage.decorations.trip_queries as esdtq
import emission.analysis.modelling.tour_model_first_only.build_save_model as bsm
import emission.analysis.modelling.tour_model_first_only.evaluation_pipeline as ep
from emission.analysis.classification.inference.labels.inferrers import predict_cluster_confidence_discounting
import emission.core.wrapper.entry as ecwe

# logging.basicConfig(level=logging.DEBUG)

EARTH_RADIUS = 6371000
RADIUS = 500


class old_clustering_predictor():
    """ temporary class that implements first round clustering so that we don't 
        have to run the whole pipeline. also adds fit and predict methods so 
        that we can use this in our custom cross-validation function. 

        NOTE: the output is not up to date (does not contain separated 
        confidences for each label category)
    """

    def __init__(self, user_id, radius=RADIUS):
        self.user_id = user_id
        self.radius = radius

    def fit(self, train_trips):
        """ copied from bsm.build_user_model()
        
            Args:
                train_trips: list or dataframe of trips
        """
        # convert train_trips to a list, if needed
        if isinstance(train_trips, pd.DataFrame):
            train_trips = self._trip_df_to_list(train_trips)

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
                tuple of lists: (mode_pred, purpose_pred, replaced_pred)
        """
        mode_pred = []
        purpose_pred = []
        replaced_pred = []
        # confidence = []

        # convert test_trips to a list, if needed
        if isinstance(test_trips, pd.DataFrame):
            test_trips = self._trip_df_to_list(test_trips)

        for trip in test_trips:
            predictions = predict_cluster_confidence_discounting(trip)

            if len(predictions) == 0:
                mode_pred.append(np.nan)
                purpose_pred.append(np.nan)
                replaced_pred.append(np.nan)
                # confidence.append(0)

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
                # confidence.append(top_conf)

        return mode_pred, purpose_pred, replaced_pred  #, confidence

    def _trip_df_to_list(self, trip_df):
        trips_list = []

        for idx, row in trip_df.iterrows():
            data = {
                'source': row['source'],
                'end_ts': row['end_ts'],
                # 'end_local_dt':row['end_local_dt'],
                'end_fmt_time': row['end_fmt_time'],
                'end_loc': row['end_loc'],
                'raw_trip': row['raw_trip'],
                'start_ts': row['start_ts'],
                # 'start_local_dt':row['start_local_dt'],
                'start_fmt_time': row['start_fmt_time'],
                'start_loc': row['start_loc'],
                'duration': row['duration'],
                'distance': row['distance'],
                'start_place': row['start_place'],
                'end_place': row['end_place'],
                'cleaned_trip': row['cleaned_trip'],
                'inferred_labels': row['inferred_labels'],
                'inferred_trip': row['inferred_trip'],
                'expectation': row['expectation'],
                'confidence_threshold': row['confidence_threshold'],
                'expected_trip': row['expected_trip'],
                'user_input': row['user_input']
            }
            trip = ecwe.Entry.create_entry(user_id=row['user_id'],
                                           key='analysis/confirmed_trip',
                                           data=data)
            trips_list += [trip]

        return trips_list


class new_clustering():
    """ Model that learns and predicts location clusters. 

        TODO: update docstrings
        Attributes: 
            radius (int): 
            train_df (df):
            test_df (df): 
            base_model (sklearn model):
            svm_models (dict): map containing the SVM model used to subdivide clusters
    """

    def __init__(self,
                 loc_type='end',
                 radius=150,
                 size_thresh=6,
                 purity_thresh=0.7,
                 gamma=0.05,
                 C=1):
        """ Args:
                loc_type (str): 'start' or 'end', the type of point to cluster
                radii (int list): list of radii to run the clustering algs with
                size_thresh (int): the min number of trips a cluster must have 
                    to be considered for SVm sub-division
                purity_thresh (float): the min purity a cluster must have 
                    to be sub-divided using SVM
                gamma (float): the gamma hyperparameter for SVM
                C (float): the C hyperparameter for SVM
        """
        self.loc_type = loc_type
        self.radius = radius
        self.size_thresh = size_thresh
        self.purity_thresh = purity_thresh
        self.gamma = gamma
        self.C = C
        self.svm_models = {}

    def fit(self, train_df):
        """ assigns every trip to a cluster. self.trips_df will be updated with 
            a new column, 'cluster_idx', which contains the cluster indices. 

            TODO: add fit_start and fit_end so that when we store the models, we don't have to store all trip data twice

            Args:
                train_df (dataframe): dataframe of labeled trips
        """
        self.train_df = self._clean_data(train_df)

        # TODO: maybe rethink this part, should it be the same for fit_start?
        # if a trip is missing mode/replaced but has purpose, we'll still use it when we train for destination clusters
        if self.train_df.purpose_true.isna().any():
            num_nan = self.train_df.purpose_true.value_counts(
                dropna=False).loc[np.nan]
            logging.info(
                f'dropping {num_nan}/{len(self.train_df)} trips that are missing purpose labels'
            )
            self.train_df = self.train_df.dropna(
                subset=['purpose_true']).reset_index(drop=True)
        if len(self.train_df) == 0:
            # i.e. no valid trips after removing all nans
            raise Exception('no valid trips; nothing to fit')

        dist_matrix_meters = clustering.get_distance_matrix(
            self.train_df, self.loc_type)
        self.base_model = DBSCAN(self.radius,
                                 metric="precomputed",
                                 min_samples=2).fit(dist_matrix_meters)
        base_clusters = self.base_model.labels_

        self.train_df.loc[:, 'base_cluster_idx'] = base_clusters

        # move "noisy" trips to their own single-trip clusters
        for idx, row in self.train_df.loc[self.train_df['base_cluster_idx'] ==
                                          -1].iterrows():
            self.train_df.loc[idx, 'base_cluster_idx'] = 1 + self.train_df[
                'base_cluster_idx'].max()

        # copy base cluster column into final cluster column. we want to preserve base clusters because they will be used in the predict method
        self.train_df.loc[:, 'final_cluster_idx'] = self.train_df[
            'base_cluster_idx']

        c = 0  # count of how many clusters we have iterated over

        # iterate over all clusters and subdivide them with SVM. the while loop is so we can do multiple iterations of subdividing if needed
        while c < self.train_df['final_cluster_idx'].max():
            points_in_cluster = self.train_df[
                self.train_df['final_cluster_idx'] == c]

            # only do SVM if we have the minimum num of trips in the cluster
            if len(points_in_cluster) < self.size_thresh:
                c += 1
                continue

            # only do SVM if purity is below threshold
            purity = clustering.single_cluster_purity(points_in_cluster,
                                                      label_col='purpose_true')
            if purity < self.purity_thresh:
                X = points_in_cluster[[
                    f"{self.loc_type}_lon", f"{self.loc_type}_lat"
                ]]
                y = points_in_cluster.purpose_true.to_list()

                svm_model = make_pipeline(
                    StandardScaler(),
                    svm.SVC(
                        kernel='rbf',
                        gamma=self.gamma,
                        C=self.C,
                    )).fit(X, y)
                labels = svm_model.predict(X)
                unique_labels = np.unique(labels)

                # if the SVM predicts that all points in the cluster have the same label, just ignore it and don't reindex.

                # this also helps us to handle the possibility that a cluster
                # may be impure but inherently inseparable, e.g. an end cluster at a user's home, containing 50% trips from work to home and 50% round trips that start and end at home. we don't want to reindex otherwise the low purity will trigger SVM again, and we will attempt & fail to split the cluster ad infinitum
                if len(unique_labels) > 1:
                    # map purpose labels to new cluster indices
                    # we offset indices by the max existing index so that we don't run into any duplicate indices
                    max_existing_idx = self.train_df['final_cluster_idx'].max()
                    label_to_cluster = {
                        unique_labels[i]: i + max_existing_idx + 1
                        for i in range(len(unique_labels))
                    }
                    # update trips with their new cluster indices
                    indices = np.array([label_to_cluster[l] for l in labels])
                    self.train_df.loc[self.train_df['final_cluster_idx'] == c,
                                      'final_cluster_idx'] = indices

                    # store the svm model in the dict, along with the label_to_cluster map so that we can calculate the correct cluster index inside predict()
                    self.svm_models[c] = (svm_model, label_to_cluster)

            c += 1
        # TODO: make things categorical at the end? or maybe at the start of the decision tree pipeline

        return self

    def predict(self, test_df):
        """ Generate predictions for all unlabeled trips (if possible). adds 3 
            new columns to self.trips_df: 'mode_pred', 'purpose_pred', 
            'replaced_pred'. The entries of these columns are dictionaries, 
            where the keys are the predicted labels and the values are the 
            associated probabilities/confidences. 

            Args:
                test_df (dataframe): dataframe of test trips

            TODO: store clusters as polygons so the prediction is faster
        """
        self.test_df = self._clean_data(test_df)
        # sklearn doesn't implement predict() for DBSCAN, so we use a custom method
        pred_base_clusters = self._dbscan_predict()

        self.test_df.loc[:, 'base_cluster_idx'] = pred_base_clusters
        self.test_df.loc[:, 'final_cluster_idx'] = pred_base_clusters

        # iterate over all clusters and check if SVM was used. the while loop is so we can do multiple iterations of subdividing if needed
        c = 0
        while c < self.test_df['final_cluster_idx'].max():
            # if c is in the set of final cluster indices, then it will be our final prediction; don't modify anything.
            if c in self.train_df['final_cluster_idx'].unique():
                c += 1
                continue

            # if c is not a final cluster idx, that must mean that it was subdivided. therefore, it should appear as a key in self.svm_models.
            assert c in self.svm_models.keys()

            points_in_cluster = self.test_df[self.test_df['final_cluster_idx']
                                             == c]
            # if we didn't predict any labels to be c, then skip
            if len(points_in_cluster) == 0:
                c += 1
                continue

            X = points_in_cluster[[
                f"{self.loc_type}_lon", f"{self.loc_type}_lat"
            ]]

            svm_model, label_to_cluster = self.svm_models[c]
            labels = svm_model.predict(X)
            unique_labels = np.unique(labels)

            # NOTE: it is possible (though from spot-checking, it appears very rare) that the set of labels the svm will predict is greater than the set of labels it predicted for the existing labeled points. (for example, Shankari's home cluster at DBSCAN+SVM, rad=150m, purity=0.7, size=6, gamma=0.05, C=1). in such a scenario, SVM may predict a final cluster label that isn't present in the 'final_cluster_idx' column of the train data, nor will it be in the keys of self.svm_models. if we wanted to predict labels for this trip, we would want to use the original base cluster rather than the SVM subcluster (because it would be the only trip in its SVM subcluster and we would not have any label information). thus, I'll keep the 'base_cluster_idx' columns for now, and assign a final_cluster_idx of -2 to indicate that such an error as occurred.
            # TODO: we can try to restrict SVM predictions to the labels it assigned during the fit() process. perhaps could create a custom svm.predict() using the model's decision functions?
            for l in unique_labels:
                if l not in label_to_cluster.keys():
                    label_to_cluster[l] = -2

            # map purpose labels to new cluster indices
            indices = np.array([label_to_cluster[l] for l in labels])
            self.test_df.loc[self.test_df['final_cluster_idx'] == c,
                             'final_cluster_idx'] = indices

            c += 1

        return self.test_df[['final_cluster_idx']]

    def _clean_data(self, df):
        """ prepare a dataframe to be used in this model. 

            Args:
                df: a dataframe
        """
        assert 'start_loc' in df.columns and 'end_loc' in df.columns

        # clean up the dataframe by dropping entries with NaN locations and
        # reset index
        num_nan = 0
        if df.start_loc.isna().any():
            num_nan += df.start_loc.value_counts(dropna=False).loc[np.nan]
            df = df.dropna(subset=['start_loc'])
        if df.end_loc.isna().any():
            num_nan += df.end_loc.value_counts(dropna=False).loc[np.nan]
            df = df.dropna(subset=['end_loc'])

        # expand the 'start_loc' and 'end_loc' column into 'start_lat',
        # 'start_lon', 'end_lat', and 'end_lon' columns
        df = data_wrangling.expand_coords(df)

        # drop trips with missing coordinates
        if df.start_lat.isna().any():
            num_nan += df.start_lat.value_counts(dropna=False).loc[np.nan]
            df = df.dropna(subset=['start_lat'])
        if df.start_lon.isna().any():
            num_nan += df.start_lon.value_counts(dropna=False).loc[np.nan]
            df = df.dropna(subset=['start_lon'])
        if df.end_lat.isna().any():
            num_nan += df.end_lat.value_counts(dropna=False).loc[np.nan]
            df = df.dropna(subset=['end_lat'])
        if df.end_lon.isna().any():
            num_nan = df.end_lon.value_counts(dropna=False).loc[np.nan]
            df += df.dropna(subset=['end_lon'])
        if num_nan > 0:
            logging.info(
                f'dropped {num_nan} trips that are missing location coordinates'
            )

        df = df.rename(
            columns={
                'mode_confirm': 'mode_true',
                'purpose_confirm': 'purpose_true',
                'replaced_mode': 'replaced_true'
            })

        for category in ['mode_true', 'purpose_true', 'replaced_true']:
            if category not in df.columns:
                # for example, if a user labels all their trip modes but none of their trip purposes
                df.loc[:, category] = np.nan

        return df.reset_index(drop=True)

    def _dbscan_predict(self, ):
        n_samples = self.test_df.shape[0]
        labels = np.ones(shape=n_samples, dtype=int) * -1

        # get coordinates of core points (we can't use model.components_ because our input feature was a distance matrix and doesn't contain info about the raw coordinates)
        # NOTE: technically, every single point in a cluster is a core point because it has at least minPts (2) points, including itself, in its radius
        train_coordinates = self.train_df[[
            f'{self.loc_type}_lat', f'{self.loc_type}_lon'
        ]]
        train_radians = np.radians(train_coordinates)

        for idx, row in self.test_df.reset_index(drop=True).iterrows():
            # calculate the distances between the ith test data and all points, then find the index of the closest point. if the ith test data is within epsilon of the point, then assign its cluster to the ith test data (otherwise, leave it as -1, indicating noise)
            # unfortunately, pairwise_distances_argmin() does not support haversine distance, so we have to reimplement it ourselves
            new_loc_radians = np.radians(
                row[[self.loc_type + "_lat",
                     self.loc_type + "_lon"]].to_list())
            new_loc_radians = np.reshape(new_loc_radians, (1, 2))
            dist_matrix_meters = smp.haversine_distances(
                new_loc_radians, train_radians) * EARTH_RADIUS

            shortest_dist_idx = np.argmin(dist_matrix_meters)
            if dist_matrix_meters[0, shortest_dist_idx] < self.radius:
                labels[idx] = self.train_df.reset_index(
                    drop=True).loc[shortest_dist_idx, 'final_cluster_idx']

        return labels


class cluster_only_predictor():
    """ Model for predicting mode, purpose, and replaced mode labels for a user's trips. Only destination cluster information is used. """

    def __init__(
            self,
            # user_id,
            # trips_df,
            radius=100,
            size_thresh=6,
            purity_thresh=0.7,
            gamma=0.05,
            C=1):
        self.cluster_model = new_clustering(loc_type='end',
                                            radius=radius,
                                            size_thresh=size_thresh,
                                            purity_thresh=purity_thresh,
                                            gamma=gamma,
                                            C=C)
        # TODO: implement start- and end-clustering
        # self.start_cluster_model = new_clustering(loc_type='start',
        #   radius=radius,
        #   size_thresh=size_thresh,
        #   purity_thresh=purity_thresh,
        #   gamma=gamma,
        #   C=C)
        # self.end_cluster_model = new_clustering(loc_type='end',
        #   radius=radius,
        #   size_thresh=size_thresh,
        #   purity_thresh=purity_thresh,
        #   gamma=gamma,
        #   C=C)

    def fit(self, train_df):
        # fit clustering model
        self.cluster_model.fit(train_df)
        # self.start_cluster_model.fit(train_df)
        # self.end_cluster_model.fit(train_df)

    def predict(self, test_df):
        """ Generate predictions for all unlabeled trips (if possible). adds 3 
             new columns to self.trips_df: 'mode_pred', 'purpose_pred', 
             'replaced_pred'. The entries of these columns are dictionaries, 
             where the keys are the predicted labels and the values are the 
             associated probabilities/confidences. 
        """
        # in this model, everything with the same end cluster will have the
        # same prediction (since we're relying solely on distribution of
        # existing labels in the cluster.) thus, we can simplify the process by
        # assigning labels to all trips in a cluster at once
        self.cluster_model.predict(test_df)
        self.cluster_model.test_df.loc[:, [
            'mode_pred', 'purpose_pred', 'replaced_pred'
        ]] = np.nan

        for c in self.cluster_model.test_df.loc[:,
                                                'final_cluster_idx'].unique():
            labeled_trips_in_cluster = self.cluster_model.train_df.loc[
                self.cluster_model.train_df.final_cluster_idx == c]
            unlabeled_trips_in_cluster = self.cluster_model.test_df.loc[
                self.cluster_model.test_df.final_cluster_idx == c]

            # get distribution of labels in this cluster
            mode_distrib = labeled_trips_in_cluster.mode_true.value_counts(
                normalize=True, dropna=True).to_dict()
            purpose_distrib = labeled_trips_in_cluster.purpose_true.value_counts(
                normalize=True, dropna=True).to_dict()
            replaced_distrib = labeled_trips_in_cluster.replaced_true.value_counts(
                normalize=True, dropna=True).to_dict()

            # TODO: add confidence discounting

            # update predictions
            # convert the dict into a list of dicts to work around pandas
            # thinking we're trying to insert information according to a
            # key-value map or something
            cluster_size = len(unlabeled_trips_in_cluster)
            self.cluster_model.test_df.loc[
                self.cluster_model.test_df.final_cluster_idx == c,
                'mode_pred'] = [mode_distrib] * cluster_size
            self.cluster_model.test_df.loc[
                self.cluster_model.test_df.final_cluster_idx == c,
                'purpose_pred'] = [purpose_distrib] * cluster_size
            self.cluster_model.test_df.loc[
                self.cluster_model.test_df.final_cluster_idx == c,
                'replaced_pred'] = [replaced_distrib] * cluster_size

        # get the highest-confidence predictions for each category
        # this is probably not the most efficient way to do things but I want to crank out some results quickly
        mode_pred = []
        purpose_pred = []
        replaced_pred = []

        for idx, row in self.cluster_model.test_df.iterrows():
            if row.mode_pred == {}:
                mode_pred.append(np.nan)
            else:
                mode_pred.append(max(row.mode_pred, key=row.mode_pred.get))
            if row.purpose_pred == {}:
                purpose_pred.append(np.nan)
            else:
                purpose_pred.append(
                    max(row.purpose_pred, key=row.purpose_pred.get))
            if row.replaced_pred == {}:
                replaced_pred.append(np.nan)
            else:
                replaced_pred.append(
                    max(row.replaced_pred, key=row.replaced_pred.get))

        return mode_pred, purpose_pred, replaced_pred


class cluster_forest_predictor():
    """ A trip classifier. 
    
        This label-assist algorithm first clusters trips by origin and destination, then applies a series of random forest models to predict trip purpose, mode, and replaced-mode. 
    
        Args:
            radius (int): radius for DBSCAN clustering
            size_thresh (int): the min number of trips a cluster must have to 
                be considered for sub-division via SVM 
            purity_thresh (float): the min purity a cluster must have to be 
                sub-divided via SVM
            gamma (float): coefficient for the rbf kernel in SVM
            C (float): regularization hyperparameter for SVM
            n_estimators (int): number of estimators in the random forest
            criterion (str): function to measure the quality of a split in the 
                random forest
            max_depth (int): max depth of a tree in the random forest. 
                unlimited if None. 
            min_samples_split (int): min number of samples required to split an 
                internal node in a decision tree
            min_samples_leaf (int): min number of samples required for a leaf 
                node in a decision tree
            max_features (str): number of features to consider when looking for the best split in a decision tree
            bootstrap (bool): whether bootstrap samples are used when building 
                decision trees
            random_state (int): random state for deterministic random forest 
                construction
    """

    # TODO: add start-location clusters

    def __init__(
            self,
            # user_id,
            # trips_df,
            radius=100,
            size_thresh=6,
            purity_thresh=0.7,
            gamma=0.05,
            C=1,
            n_estimators=100,
            criterion='gini',
            max_depth=None,
            min_samples_split=2,
            min_samples_leaf=1,
            max_features='sqrt',
            bootstrap=True,
            random_state=42):
        self.cluster_model = new_clustering(radius=radius,
                                            size_thresh=size_thresh,
                                            purity_thresh=purity_thresh,
                                            gamma=gamma,
                                            C=C)

        self.cluster_enc = OneHotEncoder(sparse=False, handle_unknown='ignore')
        # we need the imputer to handle missing purpose/mode labels when performing one-hot encoding
        self.purpose_enc = make_pipeline(
            SimpleImputer(missing_values=np.nan,
                          strategy='constant',
                          fill_value='missing'),
            OneHotEncoder(sparse=False, handle_unknown='error'))
        self.mode_enc = make_pipeline(
            SimpleImputer(missing_values=np.nan,
                          strategy='constant',
                          fill_value='missing'),
            OneHotEncoder(sparse=False, handle_unknown='error'))

        self.purpose_predictor = RandomForestClassifier(
            n_estimators=n_estimators,
            criterion=criterion,
            max_depth=max_depth,
            min_samples_split=min_samples_split,
            min_samples_leaf=min_samples_leaf,
            max_features=max_features,
            bootstrap=bootstrap,
            random_state=random_state)
        self.mode_predictor = RandomForestClassifier(
            n_estimators=n_estimators,
            criterion=criterion,
            max_depth=max_depth,
            min_samples_split=min_samples_split,
            min_samples_leaf=min_samples_leaf,
            max_features=max_features,
            bootstrap=bootstrap,
            random_state=random_state)
        self.replaced_predictor = RandomForestClassifier(
            n_estimators=n_estimators,
            criterion=criterion,
            max_depth=max_depth,
            min_samples_split=min_samples_split,
            min_samples_leaf=min_samples_leaf,
            max_features=max_features,
            bootstrap=bootstrap,
            random_state=random_state)

        self.features = [
            'duration',
            'distance',
            'start_local_dt_year',
            'start_local_dt_month',
            'start_local_dt_day',
            'start_local_dt_hour',
            # 'start_local_dt_minute',
            'start_local_dt_weekday',
            'end_local_dt_year',
            'end_local_dt_month',
            'end_local_dt_day',
            'end_local_dt_hour',
            # 'end_local_dt_minute',
            'end_local_dt_weekday',
            # 'start_lon', 'start_lat', 'end_lon', 'end_lat',
            # 'base_cluster_idx',
            # 'final_cluster_idx'
        ]
        self.targets = ['mode_true', 'purpose_true', 'replaced_true']

    def set_params(self, params):
        """ hacky code that mimics the set_params of an sklearn Estimator class 
            so that we can pass params during randomizedsearchCV 
            
            Args:
                params (dict): a dictionary where the keys are the parameter 
                names and the values are the parameter values
        """
        radius = params['radius'] if 'radius' in params.keys() else 150
        size_thresh = params['size_thresh'] if 'size_thresh' in params.keys(
        ) else 6
        purity_thresh = params[
            'purity_thresh'] if 'purity_thresh' in params.keys() else 0.7
        gamma = params['gamma'] if 'gamma' in params.keys() else 0.05
        C = params['C'] if 'C' in params.keys() else 1
        n_estimators = params['n_estimators'] if 'n_estimators' in params.keys(
        ) else 100
        criterion = params['criterion'] if 'criterion' in params.keys(
        ) else 'gini'
        max_depth = params['max_depth'] if 'max_depth' in params.keys(
        ) else None
        min_samples_split = params[
            'min_samples_split'] if 'min_samples_split' in params.keys() else 2
        min_samples_leaf = params[
            'min_samples_leaf'] if 'min_samples_leaf' in params.keys() else 1
        max_features = params['max_features'] if 'max_features' in params.keys(
        ) else 'sqrt'
        bootstrap = params['bootstrap'] if 'bootstrap' in params.keys(
        ) else True
        random_state = params['random_state'] if 'random_state' in params.keys(
        ) else 42

        # calling __init__ again is not good practice, I know...
        self.__init__(radius, size_thresh, purity_thresh, gamma, C,
                      n_estimators, criterion, max_depth, min_samples_split,
                      min_samples_leaf, max_features, bootstrap, random_state)

    def fit(self, train_df):
        """ Fit the model. 
        
            Args:
                train_df (dataframe): dataframe containing trips. must contain 
                    the following columns: 'user_input', 'start_loc', 
                    'end_loc', 'duration', 'distance', 'start_local_dt_year', 
                    'start_local_dt_month', 'start_local_dt_day', 
                    'start_local_dt_hour', 'start_local_dt_weekday', 
                    'end_local_dt_year', 'end_local_dt_month', 
                    'end_local_dt_day', 'end_local_dt_hour', 
                    'end_local_dt_weekday'

            Returns:
                self (a fitted classifier)
        """
        # fit clustering model
        self.cluster_model.fit(train_df)

        ### one-hot encode the cluster indices ###
        onehot_clusters = self.cluster_enc.fit_transform(
            self.cluster_model.train_df[[
                'base_cluster_idx', 'final_cluster_idx'
            ]])
        self.onehot_cluster_cols = [
            f'base_cluster_idx_{idx}' for idx in np.sort(
                self.cluster_model.train_df.base_cluster_idx.unique())
        ] + [
            f'final_cluster_idx_{idx}' for idx in np.sort(
                self.cluster_model.train_df.final_cluster_idx.unique())
        ]
        onehot_clusters_df = pd.DataFrame(onehot_clusters,
                                          columns=self.onehot_cluster_cols)
        self.features += self.onehot_cluster_cols
        self.cluster_model.train_df = pd.concat(
            [self.cluster_model.train_df, onehot_clusters_df], axis=1)

        ### prepare data for the random forest models ###
        # note that we want to use purpose data to aid our mode predictions, and use both purpose and mode data to aid our replaced-mode predictions
        # thus, we want to one-hot encode the purpose and mode as data features, but also preserve an unencoded copy for the target columns

        self.Xy_train = self.cluster_model.train_df[self.features +
                                                    self.targets]

        onehot_purpose = self.purpose_enc.fit_transform(
            self.Xy_train[['purpose_true']])
        self.onehot_purpose_cols = [
            f'purpose_{p}' for p in self.Xy_train['purpose_true'].unique()
        ]
        onehot_purpose_df = pd.DataFrame(onehot_purpose,
                                         columns=self.onehot_purpose_cols)
        onehot_mode = self.mode_enc.fit_transform(self.Xy_train[['mode_true']])
        self.onehot_mode_cols = [
            f'mode_{m}' for m in self.Xy_train['mode_true'].unique()
        ]
        onehot_mode_df = pd.DataFrame(onehot_mode,
                                      columns=self.onehot_mode_cols)

        self.Xy_train = pd.concat(
            [self.Xy_train, onehot_purpose_df, onehot_mode_df], axis=1)

        # for predicting purpose, drop all target labels
        self.X_purpose = self.Xy_train.dropna(subset=['purpose_true']).drop(
            labels=self.targets + self.onehot_purpose_cols +
            self.onehot_mode_cols,
            axis=1)

        # for predicting mode, we want to keep purpose data
        self.X_mode = self.Xy_train.dropna(subset=['mode_true']).drop(
            labels=self.targets + self.onehot_mode_cols, axis=1)

        # for predicting replaced-mode, we want to keep purpose and mode data
        self.X_replaced = self.Xy_train.dropna(subset=['replaced_true']).drop(
            labels=self.targets, axis=1)

        self.y_purpose = self.Xy_train['purpose_true'].dropna()
        self.y_mode = self.Xy_train['mode_true'].dropna()
        self.y_replaced = self.Xy_train['replaced_true'].dropna()

        # ideally the train data has all 3 categories of labels, but we can still train with partial labels
        # print('original train size:', len(Xy))
        # print('mode train size:', len(X_mode))
        # print('purpose train size:', len(X_purpose))
        # print('replaced train size:', len(X_replaced))

        # fit random forest models
        # print(self.X_replaced.shape, self.y_replaced.shape)
        if len(self.X_purpose) > 0:
            self.purpose_predictor.fit(self.X_purpose, self.y_purpose)
        if len(self.X_mode) > 0:
            self.mode_predictor.fit(self.X_mode, self.y_mode)
        if len(self.X_replaced) > 0:
            self.replaced_predictor.fit(self.X_replaced, self.y_replaced)

        return self

    def predict(self, test_df):
        """ Predict labels. 
        
            Args:
                test_df (dataframe): dataframe containing trips. must contain 
                    the following columns: 'start_loc', 'end_loc', 'duration', 
                    'distance', 'start_local_dt_year', 'start_local_dt_month', 
                    'start_local_dt_day', 'start_local_dt_hour', 
                    'start_local_dt_weekday', 'end_local_dt_year', 
                    'end_local_dt_month', 'end_local_dt_day', 
                    'end_local_dt_hour', 'end_local_dt_weekday'

            Returns:
                a 3-tuple, consisting of mode_pred, purpose_pred, 
                replaced_pred. 

                mode_pred, purpose_pred, replaced_pred are lists, where the ith 
                element of each list is the prediction for the ith trip in the 
                test dataframe. 
        """
        # get clusters
        self.cluster_model.predict(test_df)

        # one-hot encode the cluster indices
        onehot_clusters = self.cluster_enc.transform(
            self.cluster_model.test_df[[
                'base_cluster_idx', 'final_cluster_idx'
            ]])
        onehot_df = pd.DataFrame(onehot_clusters,
                                 columns=self.onehot_cluster_cols)
        self.cluster_model.test_df = pd.concat(
            [self.cluster_model.test_df, onehot_df], axis=1)

        # get data
        self.X_test = self.cluster_model.test_df[self.features]

        # predict purpose
        # note that we want to use purpose data to aid our mode predictions, and use both purpose and mode data to aid our replaced-mode predictions
        try:
            purpose_pred = self.purpose_predictor.predict(self.X_test)

            # update X_test with one-hot-encoded purpose predictions to aid mode predictor
            onehot_purpose = self.purpose_enc.transform(
                purpose_pred.reshape(-1, 1))
            onehot_purpose_df = pd.DataFrame(onehot_purpose,
                                             columns=self.onehot_purpose_cols)
            self.X_test = pd.concat([self.X_test, onehot_purpose_df], axis=1)

            mode_pred, replaced_pred = self._try_predict_mode_replaced()

        except NotFittedError as e:
            # if we can't predict purpose, we can still try to predict mode and replaced-mode without one-hot encoding the purpose
            # raise (e)
            purpose_pred = np.full((len(self.X_test), ), np.nan)
            mode_pred, replaced_pred = self._try_predict_mode_replaced()

        if purpose_pred.dtype == np.float64 and mode_pred.dtype == np.float64 and replaced_pred.dtype == np.float64:
            # this indicates that all the predictions are np.nan so none of the random forest classifiers were fitted
            raise NotFittedError

        # TODO: update dataframe in a better way (perhaps store dataframes directly as instance variables rather than within cluster_model)
        self.cluster_model.test_df.loc[:, 'mode_pred'] = mode_pred
        self.cluster_model.test_df.loc[:, 'purpose_pred'] = purpose_pred
        self.cluster_model.test_df.loc[:, 'replaced_pred'] = replaced_pred

        # TODO: get label probabilities
        return mode_pred, purpose_pred, replaced_pred

    def _try_predict_mode_replaced(self):
        try:
            # predict mode
            mode_pred = self.mode_predictor.predict(self.X_test)

            # update X_test with one-hot-encoded mode predictions to aid replaced-mode predictor
            onehot_mode = self.mode_enc.transform(mode_pred.reshape(-1, 1))
            onehot_mode_df = pd.DataFrame(onehot_mode,
                                          columns=self.onehot_mode_cols)
            self.X_test = pd.concat([self.X_test, onehot_mode_df], axis=1)
            replaced_pred = self._try_predict_replaced()

        except NotFittedError as e:
            mode_pred = np.full((len(self.X_test), ), np.nan)
            # if we don't have mode predictions, we *could* still try to predict replaced mode (but if the user didn't input mode labels then it's unlikely they would input replaced-mode)
            replaced_pred = self._try_predict_replaced()

        return mode_pred, replaced_pred

    def _try_predict_replaced(self):
        try:
            # predict replaced-mode
            replaced_pred = self.replaced_predictor.predict(self.X_test)
        except NotFittedError as e:
            replaced_pred = np.full((len(self.X_test), ), np.nan)
        return replaced_pred


class cluster_adaboost_predictor(cluster_forest_predictor):

    def __init__(
            self,
            # user_id,
            # trips_df,
            radius=150,
            size_thresh=6,
            purity_thresh=0.7,
            gamma=0.05,
            C=1,
            n_estimators=100,
            learning_rate=1.0,
            criterion='gini',
            max_depth=None,
            min_samples_split=2,
            min_samples_leaf=1,
            max_features='sqrt',
            random_state=42):
        self.cluster_model = new_clustering(radius=150,
                                            size_thresh=6,
                                            purity_thresh=0.7,
                                            gamma=0.05,
                                            C=1)

        self.cluster_enc = OneHotEncoder(sparse=False, handle_unknown='ignore')
        # we need the imputer to handle missing purpose/mode labels when performing one-hot encoding
        self.purpose_enc = make_pipeline(
            SimpleImputer(missing_values=np.nan,
                          strategy='constant',
                          fill_value='missing'),
            OneHotEncoder(sparse=False, handle_unknown='error'))
        self.mode_enc = make_pipeline(
            SimpleImputer(missing_values=np.nan,
                          strategy='constant',
                          fill_value='missing'),
            OneHotEncoder(sparse=False, handle_unknown='error'))

        self.purpose_predictor = AdaBoostClassifier(
            base_estimator=DecisionTreeClassifier(
                criterion=criterion,
                max_depth=max_depth,
                min_samples_split=min_samples_split,
                min_samples_leaf=min_samples_leaf,
                max_features=max_features,
            ),
            n_estimators=n_estimators,
            learning_rate=1.0,
            random_state=random_state)
        self.mode_predictor = AdaBoostClassifier(
            base_estimator=DecisionTreeClassifier(
                criterion=criterion,
                max_depth=max_depth,
                min_samples_split=min_samples_split,
                min_samples_leaf=min_samples_leaf,
                max_features=max_features,
            ),
            n_estimators=n_estimators,
            learning_rate=1.0,
            random_state=random_state)
        self.replaced_predictor = AdaBoostClassifier(
            base_estimator=DecisionTreeClassifier(
                criterion=criterion,
                max_depth=max_depth,
                min_samples_split=min_samples_split,
                min_samples_leaf=min_samples_leaf,
                max_features=max_features,
            ),
            n_estimators=n_estimators,
            learning_rate=1.0,
            random_state=random_state)

        self.features = [
            'duration',
            'distance',
            'start_local_dt_year',
            'start_local_dt_month',
            'start_local_dt_day',
            'start_local_dt_hour',
            # 'start_local_dt_minute',
            'start_local_dt_weekday',
            'end_local_dt_year',
            'end_local_dt_month',
            'end_local_dt_day',
            'end_local_dt_hour',
            # 'end_local_dt_minute',
            'end_local_dt_weekday',
            # 'start_lon', 'start_lat', 'end_lon', 'end_lat',
            # 'base_cluster_idx',
            # 'final_cluster_idx'
        ]
        self.targets = ['mode_true', 'purpose_true', 'replaced_true']

    def set_params(self, params):
        # hacky code so that we can pass params during randomizedsearchCV
        radius = params['radius'] if 'radius' in params.keys() else 150
        size_thresh = params['size_thresh'] if 'size_thresh' in params.keys(
        ) else 6
        purity_thresh = params[
            'purity_thresh'] if 'purity_thresh' in params.keys() else 0.7
        gamma = params['gamma'] if 'gamma' in params.keys() else 0.05
        C = params['C'] if 'C' in params.keys() else 1
        n_estimators = params['n_estimators'] if 'n_estimators' in params.keys(
        ) else 100
        learning_rate = params[
            'learning_rate'] if 'learning_rate' in params.keys() else 1.0
        criterion = params['criterion'] if 'criterion' in params.keys(
        ) else 'gini'
        max_depth = params['max_depth'] if 'max_depth' in params.keys(
        ) else None
        min_samples_split = params[
            'min_samples_split'] if 'min_samples_split' in params.keys() else 2
        min_samples_leaf = params[
            'min_samples_leaf'] if 'min_samples_leaf' in params.keys() else 1
        max_features = params['max_features'] if 'max_features' in params.keys(
        ) else 'sqrt'
        random_state = params['random_state'] if 'random_state' in params.keys(
        ) else 42

        # calling __init__ again is not good practice, I know...
        self.__init__(radius, size_thresh, purity_thresh, gamma, C,
                      n_estimators, learning_rate, criterion, max_depth,
                      min_samples_split, min_samples_leaf, max_features,
                      random_state)

    def fit(self, train_df):
        return super().fit(train_df)

    def predict(self, test_df):
        return super().predict(test_df)
