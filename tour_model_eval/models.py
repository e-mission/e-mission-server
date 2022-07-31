import pandas as pd
import numpy as np
import logging

# sklearn imports
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
import sklearn.metrics.pairwise as smp
from sklearn.cluster import DBSCAN
from sklearn import svm
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
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
from clustering import get_distance_matrix, single_cluster_purity
import data_wrangling
import emission.storage.decorations.trip_queries as esdtq
import emission.analysis.modelling.tour_model_first_only.build_save_model as bsm
import emission.analysis.modelling.tour_model_first_only.evaluation_pipeline as ep
from emission.analysis.classification.inference.labels.inferrers import predict_cluster_confidence_discounting
import emission.core.wrapper.entry as ecwe

# logging.basicConfig(level=logging.DEBUG)

EARTH_RADIUS = 6371000


class OldClusteringPredictor():
    """ Model that learns and predicts trip labels, using our Similarity 
        function to create trip-level clusters. 

        NOTE: the output does not contain separated confidences for each label 
        category

        Args: 
            user_id (UUID): the user's UUID
            radius (int): maximum distance between any two points in the same 
                cluster
    """

    def __init__(self, user_id, radius=500):
        self.user_id = user_id  # user_id is required for saving/retrieving the model
        self.radius = radius

    def fit(self, train_trips):
        """ Create trip-level clusters from the training data. 
            (copied from bsm.build_user_model() )
        
            Args:
                train_trips: list or dataframe of trips. must contain data on start_loc, end_loc, and user_input (a dictionary with mode_confirm, purpose_confirm, and replaced_mode)
        """
        # convert train_trips to a list, if needed, because the existing binning algorithm only accepts lists
        if isinstance(train_trips, pd.DataFrame):
            train_trips = self._trip_df_to_list(train_trips)

        sim, bins, bin_trips, train_trips = ep.first_round(
            train_trips, self.radius)

        # set instance variables so we can access results later as well
        self.sim = sim
        self.bins = bins

        # save all user labels
        bsm.save_models('user_labels',
                        bsm.create_user_input_map(train_trips, bins),
                        self.user_id)

        # save location features of all bins
        bsm.save_models('locations',
                        bsm.create_location_map(train_trips,
                                                bins), self.user_id)

    def predict(self, test_trips):
        """ Predicts trip labels by assigning trips to clusters. 
        
            Args:
                test_trips: list or dataframe of trips. must contain data on start_loc and end_loc
                
            Returns:
                tuple of lists: (mode_pred, purpose_pred, replaced_pred)
        """
        mode_pred = []
        purpose_pred = []
        replaced_pred = []
        # confidence = []

        # convert train_trips to a list, if needed, because the existing binning algorithm only accepts lists
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

                # top_conf = expand_predictions.loc[id_max, 'p']

                mode_pred.append(top_mode)
                purpose_pred.append(top_purpose)
                replaced_pred.append(top_replaced)
                # confidence.append(top_conf)

        return mode_pred, purpose_pred, replaced_pred  #, confidence

    def _trip_df_to_list(self, trip_df):
        """ Converts a dataframe of trips into a list of trip Entry objects. 

            Allows this class to accept DataFrames (which are used by the new clustering algorithms) without having to refactor the old clustering algorithm. 
        
            Args:
                trip_df: DataFrame containing trips. See code below for the 
                    expected columns. 

        """
        trips_list = []

        for idx, row in trip_df.iterrows():
            data = {
                'source': row['source'],
                'end_ts': row['end_ts'],
                # 'end_local_dt':row['end_local_dt'], # this attribute doesn't seem to appear in the dataframes I've tested with
                'end_fmt_time': row['end_fmt_time'],
                'end_loc': row['end_loc'],
                'raw_trip': row['raw_trip'],
                'start_ts': row['start_ts'],
                # 'start_local_dt':row['start_local_dt'], # this attribute doesn't seem to appear in the dataframes I've tested with
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


class DBSCANSVM_Clustering():
    """ Model that learns and predicts location clusters. 

        Args:
            loc_type (str): 'start' or 'end', the type of point to cluster
            radius (int): max distance between two points in each other's 
                neighborhood, i.e. DBSCAN's eps value. does not strictly 
                dictate final cluster size
            size_thresh (int): the min number of trips a cluster must have 
                to be considered for SVM sub-division
            purity_thresh (float): the min purity a cluster must have 
                to be sub-divided using SVM
            gamma (float): coefficient for the rbf kernel in SVM
            C (float): regularization hyperparameter for SVM

        Attributes: TODO: update this
            train_df (DataFrame):
            test_df (DataFrame): 
            base_model (sklearn model):
            svm_models (dict): map containing the SVM model used to subdivide clusters
    """

    def __init__(self,
                 loc_type='end',
                 radius=100,
                 size_thresh=6,
                 purity_thresh=0.7,
                 gamma=0.05,
                 C=1):
        self.loc_type = loc_type
        self.radius = radius
        self.size_thresh = size_thresh
        self.purity_thresh = purity_thresh
        self.gamma = gamma
        self.C = C

    def fit(self, train_df):
        """ Creates clusters of trip points. 
            self.trips_df will be updated with a new column, 'cluster_idx', 
            which contains the cluster indices. 

            TODO: perhaps move the loc_type argument to fit() so we can use a 
            single class instance to cluster both start and end points. This 
            will also help us reduce duplicate data. 

            Args:
                train_df (dataframe): dataframe of labeled trips
        """
        ##################
        ### clean data ###
        ##################
        self.train_df = self._clean_data(train_df)

        # we can use all trips as long as they have purpose labels. it's ok if
        # they're missing mode/replaced-mode labels, because they aren't as
        # strongly correlated with location compared to purpose
        # TODO: actually, we may want to rethink this. for example, it will
        # probably be helpful to include trips that are missing purpose labels
        # but still have mode labels.
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

        #########################
        ### get base clusters ###
        #########################
        dist_matrix_meters = get_distance_matrix(self.train_df, self.loc_type)
        self.base_model = DBSCAN(self.radius,
                                 metric="precomputed",
                                 min_samples=1).fit(dist_matrix_meters)
        base_clusters = self.base_model.labels_

        self.train_df.loc[:,
                          f'{self.loc_type}_base_cluster_idx'] = base_clusters

        ########################
        ### get sub-clusters ###
        ########################
        # copy base cluster column into final cluster column. we want to
        # preserve base clusters because they will be used in the predict method
        self.train_df.loc[:, f'{self.loc_type}_cluster_idx'] = self.train_df[
            f'{self.loc_type}_base_cluster_idx']

        c = 0  # count of how many clusters we have iterated over

        # iterate over all clusters and subdivide them with SVM. the while loop
        # is so we can do multiple iterations of subdividing if needed
        while c < self.train_df[f'{self.loc_type}_cluster_idx'].max():
            points_in_cluster = self.train_df[
                self.train_df[f'{self.loc_type}_cluster_idx'] == c]

            # only do SVM if we have the minimum num of trips in the cluster
            if len(points_in_cluster) < self.size_thresh:
                c += 1
                continue

            # only do SVM if purity is below threshold
            purity = single_cluster_purity(points_in_cluster,
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

                # if the SVM predicts that all points in the cluster have the
                # same label, just ignore it and don't reindex.

                # this also helps us to handle the possibility that a cluster
                # may be impure but inherently inseparable, e.g. an end cluster
                # at a user's home, containing 50% trips from work to home and
                # 50% round trips that start and end at home. we don't want to
                # reindex otherwise the low purity will trigger SVM again, and
                # we will attempt & fail to split the cluster ad infinitum
                if len(unique_labels) > 1:
                    # map purpose labels to new cluster indices
                    # we offset indices by the max existing index so that we
                    # don't run into any duplicate indices
                    max_existing_idx = self.train_df[
                        f'{self.loc_type}_cluster_idx'].max()
                    label_to_cluster = {
                        unique_labels[i]: i + max_existing_idx + 1
                        for i in range(len(unique_labels))
                    }
                    # update trips with their new cluster indices
                    indices = np.array([label_to_cluster[l] for l in labels])
                    self.train_df.loc[
                        self.train_df[f'{self.loc_type}_cluster_idx'] == c,
                        f'{self.loc_type}_cluster_idx'] = indices


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
        # TODO: we probably don't want to store test_df in self to be more memory-efficient
        self.test_df = self._clean_data(test_df)
        pred_base_clusters = self._dbscan_predict(self.test_df)

        self.test_df.loc[:,
                         f'{self.loc_type}_cluster_idx'] = pred_base_clusters

        return self.test_df[[f'{self.loc_type}_cluster_idx']]

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

    def _dbscan_predict(self, test_df):
        """ Generate base-cluster predictions for the test data using a 
            nearest-neighbor approach. 
        
            sklearn doesn't implement predict() for DBSCAN, which is why we 
            need a custom method.
        """
        n_samples = test_df.shape[0]
        labels = np.ones(shape=n_samples, dtype=int) * -1

        # get coordinates of core points (we can't use model.components_ because our input feature was a distance matrix and doesn't contain info about the raw coordinates)
        # NOTE: technically, every single point in a cluster is a core point because it has at least minPts (2) points, including itself, in its radius
        train_coordinates = self.train_df[[
            f'{self.loc_type}_lat', f'{self.loc_type}_lon'
        ]]
        train_radians = np.radians(train_coordinates)

        for idx, row in test_df.reset_index(drop=True).iterrows():
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
                    drop=True).loc[shortest_dist_idx,
                                   f'{self.loc_type}_cluster_idx']

        return labels


class ClusterOnlyPredictor():
    """ Model for predicting mode, purpose, and replaced mode labels for a user's trips. Only cluster information is used. 
    
        Args: 
            TODO: update docstring
    """

    def __init__(
            self,
            radius=100,  # TODO: add diff start and end radii
            size_thresh=6,
            purity_thresh=0.7,
            gamma=0.05,
            C=1,
            cluster_method='end'):
        assert cluster_method in ['end', 'trip', 'combination']
        self.cluster_method = cluster_method

        self.end_cluster_model = DBSCANSVM_Clustering(
            loc_type='end',
            radius=radius,
            size_thresh=size_thresh,
            purity_thresh=purity_thresh,
            gamma=gamma,
            C=C)

        if self.cluster_method in ['trip', 'combination']:
            self.start_cluster_model = DBSCANSVM_Clustering(
                loc_type='start',
                radius=radius,
                size_thresh=size_thresh,
                purity_thresh=purity_thresh,
                gamma=gamma,
                C=C)
            self.trip_grouper = TripGrouper(
                start_cluster_col='start_cluster_idx',
                end_cluster_col='end_cluster_idx')

    def set_params(self, params):
        """ hacky code that mimics the set_params of an sklearn Estimator class 
            so that we can pass params during randomizedsearchCV 
            
            Args:
                params (dict): a dictionary where the keys are the parameter 
                names and the values are the parameter values
        """
        radius = params['radius'] if 'radius' in params.keys() else 100
        size_thresh = params['size_thresh'] if 'size_thresh' in params.keys(
        ) else 6
        purity_thresh = params[
            'purity_thresh'] if 'purity_thresh' in params.keys() else 0.7
        gamma = params['gamma'] if 'gamma' in params.keys() else 0.05
        C = params['C'] if 'C' in params.keys() else 1
        cluster_method = params[
            'cluster_method'] if 'cluster_method' in params.keys() else 'end'

        # calling __init__ again is not good practice, I know...
        self.__init__(radius, size_thresh, purity_thresh, gamma, C,
                      cluster_method)

    def fit(self, train_df):
        # fit clustering model
        self.end_cluster_model.fit(train_df)
        self.train_df = self.end_cluster_model.train_df

        if self.cluster_method in ['trip', 'combination']:
            self.start_cluster_model.fit(train_df)
            self.train_df.loc[:, ['start_cluster_idx'
                                  ]] = self.start_cluster_model.train_df[[
                                      'start_cluster_idx'
                                  ]]

            # create trip-level clusters
            trip_cluster_idx = self.trip_grouper.fit_transform(self.train_df)
            self.train_df.loc[:, 'trip_cluster_idx'] = trip_cluster_idx

    def predict(self, test_df):
        """ Generate predictions for all unlabeled trips (if possible). adds 3 
             new columns to self.trips_df: 'mode_pred', 'purpose_pred', 
             'replaced_pred'. The entries of these columns are dictionaries, 
             where the keys are the predicted labels and the values are the 
             associated probabilities/confidences. 
        """
        self.end_cluster_model.predict(test_df)
        # store a copy of test_df for now (TODO: make this more efficient since the data is duplicated)
        self.test_df = self.end_cluster_model.test_df

        if self.cluster_method in ['trip', 'combination']:
            self.start_cluster_model.predict(test_df)
            # append the start cluster indices
            self.test_df.loc[:, [
                'start_cluster_idx'
            ]] = self.start_cluster_model.test_df.loc[:, ['start_cluster_idx']]

            # create trip-level clusters
            trip_cluster_idx = self.trip_grouper.transform(self.test_df)
            self.test_df.loc[:, 'trip_cluster_idx'] = trip_cluster_idx

        # generate label predictions from cluster information
        self.test_df.loc[:, ['mode_pred', 'purpose_pred', 'replaced_pred'
                             ]] = np.nan

        if self.cluster_method in ['end', 'trip']:
            cluster_col = f'{self.cluster_method}_cluster_idx'
            self.test_df = self._add_label_distributions(
                self.test_df, cluster_col)

        else:  # self.cluster_method == 'combination'combination
            # try to get label distributions from trip-level clusters first, because trip-level clusters tend to be more homogenous and will yield more accurate predictions
            self.test_df = self._add_label_distributions(
                self.test_df, 'trip_cluster_idx')

            # for trips that have an empty label-distribution after the first pass using trip clusters, try to get a distribution from the destination cluster (this includes both trips that *don't* fall into a trip cluster, as well as trips that *do* fall into a trip cluster but are missing some/all categories of labels due to missing user inputs.)

            # fill in missing label-distributions by the label_type
            # (we want to iterate by label_type rather than check cluster idx because it's possible that some trips in a trip-cluster have predictions for one label_type but not another)
            for label_type in ['mode', 'purpose', 'replaced']:
                self.test_df.loc[self.test_df[f'{label_type}_pred'] ==
                                 {}] = self._add_label_distributions(
                                     self.test_df.loc[
                                         self.test_df[f'{label_type}_pred'] ==
                                         {}],
                                     'end_cluster_idx',
                                     label_types=[label_type])

        # get the highest-confidence predictions for each category
        # this is probably not the most efficient way to do things but I want to crank out some results quickly
        mode_pred = []
        purpose_pred = []
        replaced_pred = []

        for idx, row in self.test_df.iterrows():
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

    def _add_label_distributions(self,
                                 df,
                                 cluster_col,
                                 label_types=['mode', 'purpose', 'replaced']):
        df = df.copy()  # to avoid SettingWithCopyWarning
        for c in df.loc[:, cluster_col].unique():
            labeled_trips_in_cluster = self.train_df.loc[
                self.train_df[cluster_col] == c]
            unlabeled_trips_in_cluster = df.loc[df[cluster_col] == c]

            cluster_size = len(unlabeled_trips_in_cluster)

            for label_type in label_types:
                assert label_type in ['mode', 'purpose', 'replaced']

                # get distribution of label_type labels in this cluster
                distrib = labeled_trips_in_cluster[
                    f'{label_type}_true'].value_counts(normalize=True,
                                                       dropna=True).to_dict()
                # TODO: add confidence discounting

                # update predictions
                # convert the dict into a list of dicts to work around pandas
                # thinking we're trying to insert information according to a
                # key-value map
                # TODO: this is the line throwing the set on slice warning
                df.loc[df[cluster_col] == c,
                       f'{label_type}_pred'] = [distrib] * cluster_size

        return df


class ClusterForestPredictor():
    """ A trip classifier that uses clustering and random forest. 
    
        This label-assist algorithm first clusters trips by origin and 
        destination, then applies a series of random forest models to predict 
        trip purpose, mode, and replaced-mode. 
    
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
            drop_unclustered (bool): whether or not to drop predictions for 
                trips that don't have end clusters. 
            use_start_clusters (bool): whether or not to use start clusters as 
                input features to the ensemble classifier
            use_trip_clusters (bool): whether or not to use trip-level clusters 
                as input features to the ensemble classifier
    """

    def __init__(
            self,
            radius=100,  # TODO: add different start and end radii
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
            random_state=42,
            drop_unclustered=False,
            use_start_clusters=False,
            use_trip_clusters=True):
        self.drop_unclustered = drop_unclustered
        self.use_start_clusters = use_start_clusters
        self.use_trip_clusters = use_trip_clusters

        # clustering algorithm to generate end clusters
        self.end_cluster_model = DBSCANSVM_Clustering(
            loc_type='end',
            radius=radius,
            size_thresh=size_thresh,
            purity_thresh=purity_thresh,
            gamma=gamma,
            C=C)

        if self.use_start_clusters or self.use_trip_clusters:
            # clustering algorithm to generate start clusters
            self.start_cluster_model = DBSCANSVM_Clustering(
                loc_type='start',
                radius=radius,
                size_thresh=size_thresh,
                purity_thresh=purity_thresh,
                gamma=gamma,
                C=C)

            if self.use_trip_clusters:
                # helper class to generate trip-level clusters
                self.trip_grouper = TripGrouper(
                    start_cluster_col='start_cluster_idx',
                    end_cluster_col='end_cluster_idx')

        # wrapper class to generate one-hot encodings for cluster indices,
        # purposes, and modes
        self.cluster_enc = OneHotWrapper(sparse=False, handle_unknown='ignore')
        self.purpose_enc = OneHotWrapper(impute_missing=True,
                                         sparse=False,
                                         handle_unknown='error')
        self.mode_enc = OneHotWrapper(impute_missing=True,
                                      sparse=False,
                                      handle_unknown='error')

        # ensemble classifiers for each label category
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

        # base features and targets to be used in the ensemble classifiers
        # (cluster indices will also be added as features once they are one-hot
        # encoded, along with purpose and mode when applicable)
        self.base_features = [
            'duration',
            'distance',
            'start_local_dt_year',
            'start_local_dt_month',
            'start_local_dt_day',
            'start_local_dt_hour',
            # 'start_local_dt_minute',
            'start_local_dt_weekday',
            'end_local_dt_year',  # most likely the same as the start year
            'end_local_dt_month',  # most likely the same as the start month
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
        radius = params['radius'] if 'radius' in params.keys() else 100
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
        use_start_clusters = params[
            'use_start_clusters'] if 'use_start_clusters' in params.keys(
            ) else True
        drop_unclustered = params[
            'drop_unclustered'] if 'drop_unclustered' in params.keys(
            ) else False
        use_trip_clusters = params[
            'use_trip_clusters'] if 'use_trip_clusters' in params.keys(
            ) else True

        # calling __init__ again is not good practice...
        self.__init__(radius, size_thresh, purity_thresh, gamma, C,
                      n_estimators, criterion, max_depth, min_samples_split,
                      min_samples_leaf, max_features, bootstrap, random_state,
                      drop_unclustered, use_start_clusters, use_trip_clusters)

    def fit(self, train_df):
        """ Fit the model. Cluster the trips in the training set and build a 
            forest of trees. 
        
            Args:
                train_df (dataframe): dataframe containing trips. must contain 
                    the following columns: 'user_input', 'start_loc', 
                    'end_loc', 'duration', 'distance', 'start_local_dt_year', 
                    'start_local_dt_month', 'start_local_dt_day', 
                    'start_local_dt_hour', 'start_local_dt_weekday', 
                    'end_local_dt_year', 'end_local_dt_month', 
                    'end_local_dt_day', 'end_local_dt_hour', 
                    'end_local_dt_weekday', 'mode_confirm', 'purpose_confirm', 'replaced_mode'

            Returns:
                self (a fitted classifier)
        """
        ################################################################
        ### fit clustering model(s) and one-hot encode their indices ###
        ################################################################
        # TODO: consolidate start/end_cluster_model in a single instance that
        # has a location_type parameter in the fit() method
        self.end_cluster_model.fit(train_df)

        clusters_to_encode = self.end_cluster_model.train_df[[
            'end_cluster_idx'
        ]].copy()  # copy is to avoid SettingWithCopyWarning

        if self.use_start_clusters or self.use_trip_clusters:
            self.start_cluster_model.fit(train_df)

            if self.use_start_clusters:
                clusters_to_encode = pd.concat([
                    clusters_to_encode,
                    self.start_cluster_model.train_df[['start_cluster_idx']]
                ],
                                               axis=1)
            if self.use_trip_clusters:
                start_end_clusters = pd.concat([
                    self.end_cluster_model.train_df[['end_cluster_idx']],
                    self.start_cluster_model.train_df[['start_cluster_idx']]
                ],
                                               axis=1)
                trip_cluster_idx = self.trip_grouper.fit_transform(
                    start_end_clusters)
                clusters_to_encode.loc[:,
                                       'trip_cluster_idx'] = trip_cluster_idx

        onehot_end_clusters_df = self.cluster_enc.fit_transform(
            clusters_to_encode)

        #################################################
        ### prepare data for the random forest models ###
        #################################################
        # note that we want to use purpose data to aid our mode predictions,
        # and use both purpose and mode data to aid our replaced-mode
        # predictions
        # thus, we want to one-hot encode the purpose and mode as data
        # features, but also preserve an unencoded copy for the target columns

        # dataframe holding all features and targets
        self.Xy_train = pd.concat([
            self.end_cluster_model.train_df[self.base_features + self.targets],
            onehot_end_clusters_df
        ],
                                  axis=1)
        onehot_purpose_df = self.purpose_enc.fit_transform(
            self.Xy_train[['purpose_true']], output_col_prefix='purpose')
        onehot_mode_df = self.mode_enc.fit_transform(
            self.Xy_train[['mode_true']], output_col_prefix='mode')

        self.Xy_train = pd.concat(
            [self.Xy_train, onehot_purpose_df, onehot_mode_df], axis=1)

        # for predicting purpose, drop encoded purpose and mode features, as
        # well as all target labels
        self.X_purpose = self.Xy_train.dropna(subset=['purpose_true']).drop(
            labels=self.targets + self.purpose_enc.onehot_encoding_cols +
            self.mode_enc.onehot_encoding_cols,
            axis=1)

        # for predicting mode, we want to keep purpose data
        self.X_mode = self.Xy_train.dropna(subset=['mode_true']).drop(
            labels=self.targets + self.mode_enc.onehot_encoding_cols, axis=1)

        # for predicting replaced-mode, we want to keep purpose and mode data
        self.X_replaced = self.Xy_train.dropna(subset=['replaced_true']).drop(
            labels=self.targets, axis=1)

        self.y_purpose = self.Xy_train['purpose_true'].dropna()
        self.y_mode = self.Xy_train['mode_true'].dropna()
        self.y_replaced = self.Xy_train['replaced_true'].dropna()

        ################################
        ### fit random forest models ###
        ################################
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

                mode_pred, purpose_pred, replaced_pred are pandas Series, where the ith element of the Series is the prediction for the ith trip in the test dataframe. 
                
                Note that the index of the Series does NOT match the index of test_df (the outputs have been reindexed).
        """
        ################
        ### get data ###
        ################
        self.X_test_for_purpose = self._get_X_test_for_purpose(test_df)

        ########################
        ### make predictions ###
        ########################
        # note that we want to use purpose data to aid our mode predictions,
        # and use both purpose and mode data to aid our replaced-mode
        # predictions
        try:
            purpose_pred = self.purpose_predictor.predict(
                self.X_test_for_purpose)
            self.purpose_proba_raw = self.purpose_predictor.predict_proba(
                self.X_test_for_purpose)

            # update X_test with one-hot-encoded purpose predictions to aid
            # mode predictor
            # TODO: converting purpose_pred to a DataFrame feels super
            # unnecessary, make this more efficient
            onehot_purpose_df = self.purpose_enc.transform(
                pd.DataFrame(purpose_pred).set_index(
                    self.X_test_for_purpose.index))
            self.X_test_for_mode = pd.concat(
                [self.X_test_for_purpose, onehot_purpose_df], axis=1)

            mode_pred, replaced_pred = self._try_predict_mode_replaced()

        except NotFittedError as e:
            # if we can't predict purpose, we can still try to predict mode and
            # replaced-mode without one-hot encoding the purpose
            purpose_pred = np.full((len(self.X_test_for_purpose), ), np.nan)
            self.purpose_proba_raw = np.full((len(self.X_test_for_purpose), 1),
                                             0)
            # TODO: think about if it makes more sense to set the probability
            # to 0 or nan (since there is actually no prediction taking place)

            self.X_test_for_mode = self.X_test_for_purpose
            mode_pred, replaced_pred = self._try_predict_mode_replaced()

        if purpose_pred.dtype == np.float64 and mode_pred.dtype == np.float64 and replaced_pred.dtype == np.float64:
            # this indicates that all the predictions are np.nan so none of the
            # random forest classifiers were fitted
            raise NotFittedError

        self.predictions = pd.DataFrame(
            list(zip(purpose_pred, mode_pred, replaced_pred)),
            columns=['purpose_pred', 'mode_pred', 'replaced_pred'])
        if self.drop_unclustered:
            # TODO: actually, we should only drop purpose predictions. we can
            # then impute the missing entries in the purpose feature and still
            # try to predict mode and replaced-mode without it
            self.predictions.loc[
                self.end_cluster_model.test_df['end_cluster_idx'] == -1,
                ['purpose_pred', 'mode_pred', 'replaced_pred']] = np.nan

        return self.predictions.mode_pred, self.predictions.purpose_pred, self.predictions.replaced_pred

    def get_probabilities(self,
                          # prob_thresh=0.25
                          ):
        """ Predict class probabilities for the test set passed in predict().
            
            predict() must've already been called. 
            (TODO: add error handling for this)

            Returns: 
                3 dataframes, self.purpose_proba, self.mode_proba, and 
                self.replaced_proba. 
                
                The rows of each dataframe correspond to trips from the test_df 
                passed into predict(). Columns consist of all classes for the 
                label category, with entries indicating the probability that 
                each trip has that class label. there are also columns 
                indicating the highest class probability, whether it's over the 
                0.25 confidence threshold, and whether or not the trip had an 
                end cluster. 
        
        """
        self.purpose_proba = pd.DataFrame(
            self.purpose_proba_raw, columns=self.purpose_predictor.classes_)
        self.mode_proba = pd.DataFrame(self.mode_proba_raw,
                                       columns=self.mode_predictor.classes_)
        self.replaced_proba = pd.DataFrame(
            self.replaced_proba_raw, columns=self.replaced_predictor.classes_)

        for df in [self.purpose_proba, self.mode_proba, self.replaced_proba]:
            df.loc[:, 'top'] = df.max(axis=1)
            df.loc[:, 'above thresh'] = df['top'] > 0.25
            # TODO: why does this cause an error when I try to use the
            # prob_thresh instead of 0.25?

            df.loc[:, 'has cluster'] = self.end_cluster_model.test_df[
                'end_cluster_idx'] != -1

        return self.mode_proba, self.purpose_proba, self.replaced_proba

    def _get_X_test_for_purpose(self, test_df):
        """ Do the pre-processing to get data that we can then pass into the 
            ensemble classifiers. 
        """
        # get clusters
        self.end_cluster_model.predict(test_df)
        clusters_to_encode = self.end_cluster_model.test_df[[
            'end_cluster_idx'
        ]].copy()  # copy is to avoid SettingWithCopyWarning

        if self.use_start_clusters or self.use_trip_clusters:
            self.start_cluster_model.predict(test_df)

            if self.use_start_clusters:
                clusters_to_encode = pd.concat([
                    clusters_to_encode,
                    self.start_cluster_model.test_df[['start_cluster_idx']]
                ],
                                               axis=1)
            if self.use_trip_clusters:
                start_end_clusters = pd.concat([
                    self.end_cluster_model.test_df[['end_cluster_idx']],
                    self.start_cluster_model.test_df[['start_cluster_idx']]
                ],
                                               axis=1)
                trip_cluster_idx = self.trip_grouper.transform(
                    start_end_clusters)
                clusters_to_encode.loc[:,
                                       'trip_cluster_idx'] = trip_cluster_idx

        # one-hot encode the cluster indices
        onehot_end_df = self.cluster_enc.transform(clusters_to_encode)

        # extract the desired data
        X_test = pd.concat([
            self.end_cluster_model.test_df[self.base_features], onehot_end_df
        ],
                           axis=1)
        return X_test

    def _try_predict_mode_replaced(self):
        """ Try to predict mode and replaced-mode. Handles error in case the 
            ensemble algorithms were not fitted. 
        
            Requires self.X_test_for_mode to have already been set. (These are 
            the DataFrames containing the test data to be passed into self.
            mode_predictor.) 

            Returns: mode_pred and replaced_pred, two nparrays containing 
                predictions for mode and replaced-mode respectively
        """

        try:
            # predict mode
            mode_pred = self.mode_predictor.predict(self.X_test_for_mode)
            self.mode_proba_raw = self.mode_predictor.predict_proba(
                self.X_test_for_mode)

            # update X_test with one-hot-encoded mode predictions to aid replaced-mode predictor
            onehot_mode_df = self.mode_enc.transform(
                pd.DataFrame(mode_pred).set_index(self.X_test_for_mode.index))
            self.X_test_for_replaced = pd.concat(
                [self.X_test_for_mode, onehot_mode_df], axis=1)
            replaced_pred = self._try_predict_replaced()

        except NotFittedError as e:
            mode_pred = np.full((len(self.X_test_for_mode), ), np.nan)
            self.mode_proba_raw = np.full((len(self.X_test_for_mode), 1), 0)

            # if we don't have mode predictions, we *could* still try to
            # predict replaced mode (but if the user didn't input mode labels
            # then it's unlikely they would input replaced-mode)
            self.X_test_for_replaced = self.X_test_for_mode
            replaced_pred = self._try_predict_replaced()

        return mode_pred, replaced_pred

    def _try_predict_replaced(self):
        """ Try to predict replaced mode. Handles error in case the 
            replaced_predictor was not fitted. 
        
            Requires self.X_test_for_replaced to have already been set. (This 
            is the DataFrame containing the test data to be passed into self.
            replaced_predictor.) 

            Returns: replaced_pred, an nparray containing predictions for 
                replaced-mode
        """
        try:
            # predict replaced-mode
            replaced_pred = self.replaced_predictor.predict(
                self.X_test_for_replaced)
            self.replaced_proba_raw = self.replaced_predictor.predict_proba(
                self.X_test_for_replaced
            )  # has shape (len_trips, number of replaced_mode classes)
        except NotFittedError as e:
            replaced_pred = np.full((len(self.X_test_for_replaced), ), np.nan)
            self.replaced_proba_raw = np.full(
                (len(self.X_test_for_replaced), 1), 0)
        return replaced_pred


class ClusterForestSlimPredictor(ClusterForestPredictor):
    """ This is the same as ClusterForestPredictor, just with fewer base 
        features. 
    """

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
            random_state=42,
            use_start_clusters=True):
        super().__init__(radius, size_thresh, purity_thresh, gamma, C,
                         n_estimators, criterion, max_depth, min_samples_split,
                         min_samples_leaf, max_features, bootstrap,
                         random_state, use_start_clusters)

        self.base_features = self.base_features = [
            'duration',
            'distance',
        ]


class ClusterAdaBoostPredictor(ClusterForestPredictor):

    def __init__(
            self,
            radius=100,  # TODO: add different start and end radii
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
            random_state=42,
            drop_unclustered=False,
            use_start_clusters=False,
            use_trip_clusters=True,
            use_base_clusters=True,
            learning_rate=1.0):
        # do everything the same as ClusterForestPredictor except override the classifiers with AdaBoost instead of RandomForest
        super().__init__(radius, size_thresh, purity_thresh, gamma, C,
                         random_state, drop_unclustered, use_start_clusters,
                         use_trip_clusters, use_base_clusters)
        self.purpose_predictor = AdaBoostClassifier(
            n_estimators=n_estimators,
            learning_rate=learning_rate,
            random_state=random_state,
            base_estimator=DecisionTreeClassifier(
                criterion=criterion,
                max_depth=max_depth,
                min_samples_split=min_samples_split,
                min_samples_leaf=min_samples_leaf,
                max_features=max_features,
                random_state=random_state))
        self.mode_predictor = AdaBoostClassifier(
            n_estimators=n_estimators,
            learning_rate=learning_rate,
            random_state=random_state,
            base_estimator=DecisionTreeClassifier(
                criterion=criterion,
                max_depth=max_depth,
                min_samples_split=min_samples_split,
                min_samples_leaf=min_samples_leaf,
                max_features=max_features,
                random_state=random_state))
        self.replaced_predictor = AdaBoostClassifier(
            n_estimators=n_estimators,
            learning_rate=learning_rate,
            random_state=random_state,
            base_estimator=DecisionTreeClassifier(
                criterion=criterion,
                max_depth=max_depth,
                min_samples_split=min_samples_split,
                min_samples_leaf=min_samples_leaf,
                max_features=max_features,
                random_state=random_state))

    def set_params(self, params):
        """ hacky code that mimics the set_params of an sklearn Estimator class 
            so that we can pass params during randomizedsearchCV 
            
            Args:
                params (dict): a dictionary where the keys are the parameter 
                names and the values are the parameter values
        """
        radius = params['radius'] if 'radius' in params.keys() else 100
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
        max_depth = params['max_depth'] if 'max_depth' in params.keys() else 1
        min_samples_split = params[
            'min_samples_split'] if 'min_samples_split' in params.keys() else 2
        min_samples_leaf = params[
            'min_samples_leaf'] if 'min_samples_leaf' in params.keys() else 1
        max_features = params['max_features'] if 'max_features' in params.keys(
        ) else 'sqrt'
        random_state = params['random_state'] if 'random_state' in params.keys(
        ) else 42
        use_start_clusters = params[
            'use_start_clusters'] if 'use_start_clusters' in params.keys(
            ) else True
        drop_unclustered = params[
            'drop_unclustered'] if 'drop_unclustered' in params.keys(
            ) else False
        use_trip_clusters = params[
            'use_trip_clusters'] if 'use_trip_clusters' in params.keys(
            ) else True
        learning_rate = params[
            'learning_rate'] if 'learning_rate' in params.keys() else 1.0

        # calling __init__ again is not good practice, I know...
        self.__init__(radius, size_thresh, purity_thresh, gamma, C,
                      n_estimators, criterion, max_depth, min_samples_split,
                      min_samples_leaf, max_features, random_state,
                      drop_unclustered, use_start_clusters, use_trip_clusters,
                      learning_rate)


class BasicForestPredictor(ClusterForestPredictor):

    def __init__(
        self,
        n_estimators=100,
        criterion='gini',
        max_depth=None,
        min_samples_split=2,
        min_samples_leaf=1,
        max_features='sqrt',
        bootstrap=True,
        random_state=42,
    ):
        # wrapper class to generate one-hot encodings for cluster indices,
        # purposes, and modes
        self.purpose_enc = OneHotWrapper(impute_missing=True,
                                         sparse=False,
                                         handle_unknown='error')
        self.mode_enc = OneHotWrapper(impute_missing=True,
                                      sparse=False,
                                      handle_unknown='error')

        # ensemble classifiers for each label category
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

        # base features and targets to be used in the ensemble classifiers
        # (cluster indices will also be added as features once they are one-hot
        # encoded, along with purpose and mode when applicable)
        self.base_features = [
            'duration',
            'distance',
            'start_local_dt_year',
            'start_local_dt_month',
            'start_local_dt_day',
            'start_local_dt_hour',
            # 'start_local_dt_minute',
            'start_local_dt_weekday',
            'end_local_dt_year',  # most likely the same as the start year
            'end_local_dt_month',  # most likely the same as the start month
            'end_local_dt_day',
            'end_local_dt_hour',
            # 'end_local_dt_minute',
            'end_local_dt_weekday',
            'start_lon',
            'start_lat',
            'end_lon',
            'end_lat',
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

        # calling __init__ again is not good practice...
        self.__init__(n_estimators, criterion, max_depth, min_samples_split,
                      min_samples_leaf, max_features, bootstrap, random_state)

    def fit(self, train_df):
        """ Fit the model. Cluster the trips in the training set and build a 
            forest of trees. 
        
            Args:
                train_df (dataframe): dataframe containing trips. must contain 
                    the following columns: 'user_input', 'start_loc', 
                    'end_loc', 'duration', 'distance', 'start_local_dt_year', 
                    'start_local_dt_month', 'start_local_dt_day', 
                    'start_local_dt_hour', 'start_local_dt_weekday', 
                    'end_local_dt_year', 'end_local_dt_month', 
                    'end_local_dt_day', 'end_local_dt_hour', 
                    'end_local_dt_weekday', 'mode_confirm', 'purpose_confirm', 'replaced_mode'

            Returns:
                self (a fitted classifier)
        """
        #################################################
        ### prepare data for the random forest models ###
        #################################################
        # note that we want to use purpose data to aid our mode predictions,
        # and use both purpose and mode data to aid our replaced-mode
        # predictions
        # thus, we want to one-hot encode the purpose and mode as data
        # features, but also preserve an unencoded copy for the target columns

        # hacky way to reuse code, fix this
        train_df = DBSCANSVM_Clustering()._clean_data(train_df)
        if train_df.purpose_true.isna().any():
            num_nan = train_df.purpose_true.value_counts(
                dropna=False).loc[np.nan]
            logging.info(
                f'dropping {num_nan}/{len(train_df)} trips that are missing purpose labels'
            )
            train_df = train_df.dropna(subset=['purpose_true']).reset_index(
                drop=True)
        if len(train_df) == 0:
            # i.e. no valid trips after removing all nans
            raise Exception('no valid trips; nothing to fit')

        # dataframe holding all features and targets
        self.Xy_train = pd.concat([
            train_df[self.base_features + self.targets],
        ],
                                  axis=1)

        onehot_purpose_df = self.purpose_enc.fit_transform(
            self.Xy_train[['purpose_true']], output_col_prefix='purpose')
        onehot_mode_df = self.mode_enc.fit_transform(
            self.Xy_train[['mode_true']], output_col_prefix='mode')

        self.Xy_train = pd.concat(
            [self.Xy_train, onehot_purpose_df, onehot_mode_df], axis=1)

        # for predicting purpose, drop encoded purpose and mode features, as
        # well as all target labels
        self.X_purpose = self.Xy_train.dropna(subset=['purpose_true']).drop(
            labels=self.targets + self.purpose_enc.onehot_encoding_cols +
            self.mode_enc.onehot_encoding_cols,
            axis=1)

        # for predicting mode, we want to keep purpose data
        self.X_mode = self.Xy_train.dropna(subset=['mode_true']).drop(
            labels=self.targets + self.mode_enc.onehot_encoding_cols, axis=1)

        # for predicting replaced-mode, we want to keep purpose and mode data
        self.X_replaced = self.Xy_train.dropna(subset=['replaced_true']).drop(
            labels=self.targets, axis=1)

        self.y_purpose = self.Xy_train['purpose_true'].dropna()
        self.y_mode = self.Xy_train['mode_true'].dropna()
        self.y_replaced = self.Xy_train['replaced_true'].dropna()

        ################################
        ### fit random forest models ###
        ################################
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

                mode_pred, purpose_pred, replaced_pred are pandas Series, where the ith element of the Series is the prediction for the ith trip in the test dataframe. 
                
                Note that the index of the Series does NOT match the index of test_df (the outputs have been reindexed).
        """
        ################
        ### get data ###
        ################
        self.X_test_for_purpose = DBSCANSVM_Clustering()._clean_data(
            test_df).loc[:, self.base_features]

        ########################
        ### make predictions ###
        ########################
        # note that we want to use purpose data to aid our mode predictions,
        # and use both purpose and mode data to aid our replaced-mode
        # predictions
        try:
            purpose_pred = self.purpose_predictor.predict(
                self.X_test_for_purpose)
            self.purpose_proba_raw = self.purpose_predictor.predict_proba(
                self.X_test_for_purpose)

            # update X_test with one-hot-encoded purpose predictions to aid
            # mode predictor
            # TODO: converting purpose_pred to a DataFrame feels super
            # unnecessary, make this more efficient
            onehot_purpose_df = self.purpose_enc.transform(
                pd.DataFrame(purpose_pred).set_index(
                    self.X_test_for_purpose.index))
            self.X_test_for_mode = pd.concat(
                [self.X_test_for_purpose, onehot_purpose_df], axis=1)

            mode_pred, replaced_pred = self._try_predict_mode_replaced()

        except NotFittedError as e:
            # if we can't predict purpose, we can still try to predict mode and
            # replaced-mode without one-hot encoding the purpose
            purpose_pred = np.full((len(self.X_test_for_purpose), ), np.nan)
            self.purpose_proba_raw = np.full((len(self.X_test_for_purpose), 1),
                                             0)
            # TODO: think about if it makes more sense to set the probability
            # to 0 or nan (since there is actually no prediction taking place)

            self.X_test_for_mode = self.X_test_for_purpose
            mode_pred, replaced_pred = self._try_predict_mode_replaced()

        if purpose_pred.dtype == np.float64 and mode_pred.dtype == np.float64 and replaced_pred.dtype == np.float64:
            # this indicates that all the predictions are np.nan so none of the
            # random forest classifiers were fitted
            raise NotFittedError

        self.predictions = pd.DataFrame(
            list(zip(purpose_pred, mode_pred, replaced_pred)),
            columns=['purpose_pred', 'mode_pred', 'replaced_pred'])

        return self.predictions.mode_pred, self.predictions.purpose_pred, self.predictions.replaced_pred


class TripGrouper():

    def __init__(self,
                 start_cluster_col='start_cluster_idx',
                 end_cluster_col='end_cluster_idx'):
        self.start_cluster_col = start_cluster_col
        self.end_cluster_col = end_cluster_col

    def fit_transform(self, trip_df):
        trip_groups = trip_df.groupby(
            [self.start_cluster_col, self.end_cluster_col])

        # need dict so we can access the trip indices of all the trips in each group. the key is the group tuple and the value is the list of trip indices in the group.
        self.trip_groups_dict = dict(trip_groups.groups)

        # we want to convert trip-group tuples to to trip-cluster indices, hence the pd Series
        trip_groups_series = pd.Series(list(self.trip_groups_dict.keys()))

        trip_cluster_idx = np.empty(len(trip_df))

        for group_idx in range(len(trip_groups_series)):
            group_tuple = trip_groups_series[group_idx]
            trip_idxs_in_group = self.trip_groups_dict[group_tuple]
            trip_cluster_idx[trip_idxs_in_group] = group_idx

        return trip_cluster_idx

    def transform(self, new_trip_df):
        prediction_trip_groups = new_trip_df.groupby(
            [self.start_cluster_col, self.end_cluster_col])

        # need dict so we can access the trip indices of all the trips in each group. the key is the group tuple and the value is the list of trip indices in the group.
        prediction_trip_groups_dict = dict(prediction_trip_groups.groups)
        trip_groups_series = pd.Series(list(self.trip_groups_dict.keys()))
        trip_cluster_idx = np.empty(len(new_trip_df))

        for group_tuple in dict(prediction_trip_groups.groups).keys():
            # check if the trip cluster exists in the training set
            trip_idxs_in_group = prediction_trip_groups_dict[group_tuple]
            if group_tuple in self.trip_groups_dict.keys():
                # look up the group index from the series we created when we fit the model
                group_idx = trip_groups_series[trip_groups_series ==
                                               group_tuple].index[0]
            else:
                group_idx = -1

            trip_cluster_idx[trip_idxs_in_group] = group_idx

        return trip_cluster_idx


class OneHotWrapper():

    def __init__(
        self,
        impute_missing=False,
        sparse=False,
        handle_unknown='ignore',
    ):
        """ """
        if impute_missing:
            self.encoder = make_pipeline(
                SimpleImputer(missing_values=np.nan,
                              strategy='constant',
                              fill_value='missing'),
                OneHotEncoder(sparse=False, handle_unknown=handle_unknown))
        else:
            self.encoder = OneHotEncoder(sparse=sparse,
                                         handle_unknown=handle_unknown)
        # self.input_col = input_col
        # self.output_col_prefix = output_col_prefix

    def fit_transform(self, train_df, output_col_prefix=None):
        """ Args: 
                train_series: e.g. train_df['end_cluster_idx']) 
                output_col_prefix (str): only if train_df is a single column
        """
        # TODO: handle pd series
        onehot_encoding = self.encoder.fit_transform(train_df)
        self.onehot_encoding_cols = []
        for col in train_df.columns:
            if train_df.shape[1] > 1 or output_col_prefix is None:
                output_col_prefix = col
            self.onehot_encoding_cols += [
                f'{output_col_prefix}_{val}'
                for val in np.sort(train_df[col].dropna().unique())
            ]
            # handle np.nan separately because it is of type float, and may cause issues with np.sort if the rest of the unique values are strings
            if any((train_df[col].isna())):
                self.onehot_encoding_cols += [f'{output_col_prefix}_nan']

        onehot_encoding_df = pd.DataFrame(
            onehot_encoding,
            columns=self.onehot_encoding_cols).set_index(train_df.index)
        return onehot_encoding_df

    def transform(self, test_df):
        # TODO: rename test_df, this one doesn't necessarily need to be a df
        onehot_encoding = self.encoder.transform(test_df)
        onehot_encoding_df = pd.DataFrame(
            onehot_encoding,
            columns=self.onehot_encoding_cols).set_index(test_df.index)
        return onehot_encoding_df