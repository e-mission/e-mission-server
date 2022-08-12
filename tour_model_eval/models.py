import pandas as pd
import numpy as np
from abc import ABCMeta, abstractmethod  # to define abstract class "blueprints"
import logging
import copy

# sklearn imports
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics.pairwise import haversine_distances
from sklearn.cluster import DBSCAN
from sklearn import svm
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.exceptions import NotFittedError

# our imports
from clustering import get_distance_matrix, single_cluster_purity
import data_wrangling
import emission.storage.decorations.trip_queries as esdtq
import emission.analysis.modelling.tour_model_first_only.build_save_model as bsm
import emission.analysis.modelling.tour_model_first_only.evaluation_pipeline as ep
from emission.analysis.classification.inference.labels.inferrers import predict_cluster_confidence_discounting
import emission.core.wrapper.entry as ecwe
import emission.analysis.modelling.tour_model_extended.similarity as eamts

# logging.basicConfig(level=logging.DEBUG)

EARTH_RADIUS = 6371000

#############################
## define abstract classes ##
#############################


class SetupMixin(metaclass=ABCMeta):
    """ class containing code to be reused when setting up estimators. """

    @abstractmethod
    def set_params(self, params):
        """ Set the parameters of the estimator.  

            Args: 
                params (dict): dictionary where the keys are the param names (strings) and the values are the parameter inputs
            
            Returns:
                self
        """
        raise NotImplementedError

    def _clean_data(self, df):
        """ Clean a dataframe of trips. 
            (Drop trips with missing start/end locations, expand the user input 
            columns, ensure all essential columns are present)

            Args:
                df: a dataframe of trips. must contain the columns 'start_loc', 'end_loc', and should also contain the user input columns ('mode_confirm', 'purpose_confirm', 'replaced_mode') if available
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


class Cluster(SetupMixin, metaclass=ABCMeta):
    """ blueprint for clustering models. """

    @abstractmethod
    def fit(self, train_df):
        """ Fit the clustering algorithm.  
        
            Args: 
                train_df (DataFrame): dataframe of labeled trips
            
            Returns:
                self
        """
        raise NotImplementedError

    @abstractmethod
    def predict(self, test_df):
        """ Predict cluster indices for trips, if possible. Trips that could 
            not be clustered will have the index -1. 

            Args: 
                test_df (DataFrame): dataframe of test trips
            
            Returns:
                pd DataFrame containing one column, 'start_cluster_idx' or 'end_cluster_idx'
        """
        raise NotImplementedError

    def fit_predict(self, train_df):
        """ Fit the clustering algorithm and predict cluster indices for trips, 
            if possible. Trips that could not be clustered will have the index -1. 

            Args: 
                train_df (DataFrame): dataframe of labeled trips
            
            Returns:
                pd DataFrame containing one column, 'start_cluster_idx' or 'end_cluster_idx'
        """
        self.fit(train_df)
        return self.predict(train_df)


class TripClassifier(SetupMixin, metaclass=ABCMeta):

    @abstractmethod
    def fit(self, train_df):
        """ Fit a classification model.  
        
            Args: 
                train_df (DataFrame): dataframe of labeled trips
            
            Returns:
                self
        """
        raise NotImplementedError

    def predict(self, test_df):
        """ Predict trip labels.  
        
            Args: 
                test_df (DataFrame): dataframe of trips
            
            Returns:
                DataFrame containing the following columns: 
                    'purpose_pred', 'mode_pred', 'replaced_pred', 'purpose_proba', 'mode_proba', 'replaced_proba'
                the *_pred columns contain the most-likely label prediction (string for a label or float for np.nan). 
                the *_proba columns contain the probability of the most-likely prediction. 
        """
        proba_df = self.predict_proba(test_df)
        prediction_df = proba_df.loc[:, [('purpose', 'top_pred'),
                                         ('purpose', 'top_proba'),
                                         ('mode', 'top_pred'),
                                         ('mode', 'top_proba'),
                                         ('replaced', 'top_pred'),
                                         ('replaced', 'top_proba')]]

        prediction_df = prediction_df.rename(
            columns={
                ('purpose', 'top_pred'): 'purpose_pred',
                ('purpose', 'top_proba'): 'purpose_proba',
                ('mode', 'top_pred'): 'mode_pred',
                ('mode', 'top_proba'): 'mode_proba',
                ('replaced', 'top_pred'): 'replaced_pred',
                ('replaced', 'top_proba'): 'replaced_proba',
            })

        return prediction_df

    def fit_predict(self, train_df):
        """ Fit a classification model and predict trip labels.  
        
            Args: 
                train_df (DataFrame): dataframe of labeled trips
            
            Returns:
                DataFrame containing the following columns: 
                    'purpose_pred', 'mode_pred', 'replaced_pred', 'purpose_proba', 'mode_proba', 'replaced_proba'
                the *_pred columns contain the most-likely label prediction (string for a label or float for np.nan). 
                the *_proba columns contain the probability of the most-likely prediction. 
        """
        self.fit(train_df)
        return self.predict(train_df)

    @abstractmethod
    def predict_proba(self, test_df):
        """ Predict class probabilities for each trip.  
        
            Args: 
                test_df (DataFrame): dataframe of trips
            
            Returns:
                DataFrame with multiindexing. Each row represents a trip. There are 3 columns at level 1, one for each label category ('purpose', 'mode', 'replaced'). Within each category, there is a column for each label, with the row's entry being the probability that the trip has the label. There are three additional columns within each category, one indicating the most-likely label, one indicating the probability of the most-likely label, and one indicating whether or not the trip can be clustered. 
                TODO: add a fourth optional column for the number of trips in the cluster (if clusterable)

                Level 1 columns are: purpose, mode, replaced
                Lebel 2 columns are: 
                    <purpose1>, <purpose2>, ... top_pred, top_proba, clusterable
                    <mode1>, <mode2>, ... top_pred, top_proba, clusterable
                    <replaced1>, <replaced2>, ... top_pred, top_proba, clusterable
        """
        raise NotImplementedError


########################
## clustering classes ##
########################


class RefactoredNaiveCluster(Cluster):
    """ Naive fixed-width clustering algorithm. 
        Refactored from the existing Similarity class to take in dataframes for 
        consistency, and allows for separate clustering of start and end 
        clusters. 

        WARNING: this algorithm is *extremely* slow. 

        Args:
            loc_type (str): 'start' or 'end', the type of point to cluster
            radius (int): max distance between all pairs of points in a 
                cluster, i.e. strict maximum cluster width. 

        Attributes: 
            loc_type (str)
            radius (int)
            train_df (DataFrame)
            test_df (DataFrame)
            sim_model (Similarity object)
    """

    def __init__(self, loc_type='end', radius=100):
        self.loc_type = loc_type
        self.radius = radius

    def set_params(self, params):
        if 'loc_type' in params.keys(): self.loc_type = params['loc_type']
        if 'radius' in params.keys(): self.radius = params['radius']

        return self

    def fit(self, train_df):
        # clean data
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

        # fit the bins
        self.sim_model = eamts.Similarity(self.train_df,
                                          radius_start=self.radius,
                                          radius_end=self.radius,
                                          shouldFilter=False,
                                          cutoff=False)
        # we only bin the loc_type points to speed up the alg. avoid
        # unnecessary binning since this is really slow
        self.sim_model.bin_helper(loc_type=self.loc_type)
        labels = self.sim_model.data_df[self.loc_type + '_bin'].to_list()
        self.train_df.loc[:, f'{self.loc_type}_cluster_idx'] = labels
        return self

    def predict(self, test_df):
        self.test_df = self._clean_data(test_df)

        if self.loc_type == 'start':
            bins = self.sim_model.start_bins
        elif self.loc_type == 'end':
            bins = self.sim_model.end_bins

        labels = []

        # for each trip in the test list:
        for idx, row in self.test_df.iterrows():
            # iterate over all bins
            trip_binned = False
            for i, bin in enumerate(bins):
                # check if the trip can fit in the bin
                # if so, get the bin index
                if self._match(row, bin, self.loc_type):
                    labels += [i]
                    trip_binned = True
                    break
                # if not, return -1
            if not trip_binned:
                labels += [-1]

        self.test_df.loc[:, f'{self.loc_type}_cluster_idx'] = labels

        return self.test_df[[f'{self.loc_type}_cluster_idx']]

    def _match(self, trip, bin, loc_type):
        """ Check if a trip can fit into an existing bin. 
        
            copied from the Similarity class on the e-mission-server. 
        """
        for t_idx in bin:
            trip_in_bin = self.train_df.iloc[t_idx]
            if not self._distance_helper(trip, trip_in_bin, loc_type):
                return False
        return True

    def _distance_helper(self, tripa, tripb, loc_type):
        """ Check if two trips have start/end points within the distance 
            threshold. 
        
            copied from the Similarity class on the e-mission-server. 
        """
        pta_lat = tripa[[loc_type + '_lat']]
        pta_lon = tripa[[loc_type + '_lon']]
        ptb_lat = tripb[[loc_type + '_lat']]
        ptb_lon = tripb[[loc_type + '_lon']]

        return eamts.within_radius(pta_lat, pta_lon, ptb_lat, ptb_lon,
                                   self.radius)


class DBSCANSVMCluster(Cluster):
    """ DBSCAN-based clustering algorithm that optionally implements SVM 
        sub-clustering. 

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

        Attributes: 
            loc_type (str)
            radius (int)
            size_thresh (int)
            purity_thresh (float)
            gamma (float)
            C (float)
            train_df (DataFrame)
            test_df (DataFrame)
            base_model (sklearn Estimator)
    """

    def __init__(self,
                 loc_type='end',
                 radius=100,
                 svm=True,
                 size_thresh=1,
                 purity_thresh=1.0,
                 gamma=0.05,
                 C=1):
        self.loc_type = loc_type
        self.radius = radius
        self.svm = svm
        self.size_thresh = size_thresh
        self.purity_thresh = purity_thresh
        self.gamma = gamma
        self.C = C

    def set_params(self, params):
        if 'loc_type' in params.keys(): self.loc_type = params['loc_type']
        if 'radius' in params.keys(): self.radius = params['radius']
        if 'svm' in params.keys(): self.svm = params['svm']
        if 'size_thresh' in params.keys():
            self.size_thresh = params['size_thresh']
        if 'purity_thresh' in params.keys():
            self.purity_thresh = params['purity_thresh']
        if 'gamma' in params.keys(): self.gamma = params['gamma']

        return self

    def fit(self, train_df):
        """ Creates clusters of trip points. 
            self.train_df will be updated with columns containing base and final clusters. 

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
        # copy base cluster column into final cluster column
        self.train_df.loc[:, f'{self.loc_type}_cluster_idx'] = self.train_df[
            f'{self.loc_type}_base_cluster_idx']

        if self.svm:
            c = 0  # count of how many clusters we have iterated over

            # iterate over all clusters and subdivide them with SVM. the while
            # loop is so we can do multiple iterations of subdividing if needed
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

                    # if the SVM predicts that all points in the cluster have
                    # the same label, just ignore it and don't reindex.
                    # this also helps us to handle the possibility that a
                    # cluster may be impure but inherently inseparable, e.g. an
                    # end cluster at a user's home, containing 50% trips from
                    # work to home and 50% round trips that start and end at
                    # home. we don't want to reindex otherwise the low purity
                    # will trigger SVM again, and we will attempt & fail to
                    # split the cluster ad infinitum
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
                        indices = np.array(
                            [label_to_cluster[l] for l in labels])
                        self.train_df.loc[
                            self.train_df[f'{self.loc_type}_cluster_idx'] == c,
                            f'{self.loc_type}_cluster_idx'] = indices

                c += 1
        # TODO: make things categorical at the end? or maybe at the start of the decision tree pipeline

        return self

    def fit_predict(self, train_df):
        """ Override to avoid unnecessarily computation of distance matrices. 
        """
        self.fit(train_df)
        return self.train_df[[f'{self.loc_type}_cluster_idx']]

    def predict(self, test_df):
        # TODO: store clusters as polygons so the prediction is faster
        # TODO: we probably don't want to store test_df in self to be more memory-efficient
        self.test_df = self._clean_data(test_df)
        pred_clusters = self._NN_predict(self.test_df)

        self.test_df.loc[:, f'{self.loc_type}_cluster_idx'] = pred_clusters

        return self.test_df[[f'{self.loc_type}_cluster_idx']]

    def _NN_predict(self, test_df):
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
            dist_matrix_meters = haversine_distances(
                new_loc_radians, train_radians) * EARTH_RADIUS

            shortest_dist_idx = np.argmin(dist_matrix_meters)
            if dist_matrix_meters[0, shortest_dist_idx] < self.radius:
                labels[idx] = self.train_df.reset_index(
                    drop=True).loc[shortest_dist_idx,
                                   f'{self.loc_type}_cluster_idx']

        return labels


######################
## trip classifiers ##
######################


class NaiveBinningClassifier(TripClassifier):
    """ Trip classifier using the existing Similarity class and associated 
        functions without refactoring them. Essentially a wrapper for the 
        existing code on e-mission-server.

        Args: 
            radius (int): maximum distance between any two points in the same 
                cluster
    """

    def __init__(self, radius=500):
        self.radius = radius

    def set_params(self, params):
        if 'radius' in params.keys(): self.radius = params['radius']

        return self

    def fit(self, train_df):
        # (copied from bsm.build_user_model())

        # convert train_df to a list because the existing binning algorithm
        # only accepts lists of Entry objects
        train_trips = self._trip_df_to_list(train_df)

        sim, bins, bin_trips, train_trips = ep.first_round(
            train_trips, self.radius)

        # set instance variables so we can access results later as well
        self.sim = sim
        self.bins = bins

        # save all user labels
        user_id = train_df.user_id[0]
        bsm.save_models('user_labels',
                        bsm.create_user_input_map(train_trips, bins), user_id)

        # save location features of all bins
        bsm.save_models('locations',
                        bsm.create_location_map(train_trips, bins), user_id)
        return self

    def predict_proba(self, test_df):
        # convert test_df to a list because the existing binning algorithm
        # only accepts lists of Entry objects
        test_trips = self._trip_df_to_list(test_df)

        purpose_distribs = []
        mode_distribs = []
        replaced_distribs = []

        for trip in test_trips:
            trip_prediction = predict_cluster_confidence_discounting(trip)

            if len(trip_prediction) == 0:
                # model could not find cluster for the trip
                purpose_distribs += [{}]
                mode_distribs += [{}]
                replaced_distribs += [{}]

            else:
                trip_prediction_df = pd.DataFrame(trip_prediction).rename(
                    columns={'labels': 'user_input'})
                # renaming is simply so we can use the expand_userinputs
                # function

                expand_prediction = esdtq.expand_userinputs(trip_prediction_df)
                # converts the 'labels' dictionaries into individual columns

                # sum up probability for each label
                for label_type, label_distribs in zip(
                    ['purpose_confirm', 'mode_confirm', 'replaced_mode'],
                    [purpose_distribs, mode_distribs, replaced_distribs]):
                    label_distrib = {}
                    if label_type in expand_prediction.columns:
                        for label in expand_prediction[label_type].unique():
                            label_distrib[label] = expand_prediction.loc[
                                expand_prediction[label_type] == label,
                                'p'].sum()
                    label_distribs += [label_distrib]

        proba_dfs = []
        for label_type, label_distribs in zip(
            ['purpose', 'mode', 'replaced'],
            [purpose_distribs, mode_distribs, replaced_distribs]):

            proba = pd.DataFrame(label_distribs)
            proba['clusterable'] = proba.sum(axis=1) > 0
            proba['top_pred'] = proba.drop(columns=['clusterable']).idxmax(
                axis=1)
            proba['top_proba'] = proba.drop(
                columns=['clusterable', 'top_pred']).max(axis=1, skipna=True)
            classes = proba.columns[:-3]
            proba.loc[:, classes] = proba.loc[:, classes].fillna(0)
            proba = pd.concat([proba], keys=[label_type], axis=1)
            proba_dfs += [proba]

        self.proba_df = pd.concat(proba_dfs, axis=1)
        return self.proba_df

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


class ClusterExtrapolationClassifier(TripClassifier):
    """ Classifier that extrapolates labels from a trip's cluster. 
    
        Args: 
            alg (str): clustering algorithm to use; either 'DBSCAN' or 'naive'
            radius (int): radius for the clustering algorithm
            svm (bool): whether or not to use SVM sub-clustering. (only when 
                alg=='DBSCAN')
            size_thresh (int): the min number of trips a cluster must have 
                to be considered for SVM sub-division
            purity_thresh (float): the min purity a cluster must have 
                to be sub-divided using SVM
            gamma (float): coefficient for the rbf kernel in SVM
            C (float): regularization hyperparameter for SVM
            cluster_method (str): 'end', 'trip', 'combination'. whether to extrapolate labels from only end clusters, only trip clusters, or 
                both end and trip clusters when available.
    """

    def __init__(
            self,
            alg='DBSCAN',
            radius=100,  # TODO: add diff start and end radii
            svm=True,
            size_thresh=1,
            purity_thresh=1.0,
            gamma=0.05,
            C=1,
            cluster_method='end'):
        assert cluster_method in ['end', 'trip', 'combination']
        assert alg in ['DBSCAN', 'naive']
        self.alg = alg
        self.radius = radius
        self.svm = svm
        self.size_thresh = size_thresh
        self.purity_thresh = purity_thresh
        self.gamma = gamma
        self.C = C
        self.cluster_method = cluster_method

        if self.alg == 'DBSCAN':
            self.end_cluster_model = DBSCANSVMCluster(
                loc_type='end',
                radius=self.radius,
                svm=self.svm,
                size_thresh=self.size_thresh,
                purity_thresh=self.purity_thresh,
                gamma=self.gamma,
                C=self.C)
        elif self.alg == 'naive':
            self.end_cluster_model = RefactoredNaiveCluster(loc_type='end',
                                                            radius=self.radius)

        if self.cluster_method in ['trip', 'combination']:
            if self.alg == 'DBSCAN':
                self.start_cluster_model = DBSCANSVMCluster(
                    loc_type='start',
                    radius=self.radius,
                    svm=self.svm,
                    size_thresh=self.size_thresh,
                    purity_thresh=self.purity_thresh,
                    gamma=self.gamma,
                    C=self.C)
            elif self.alg == 'naive':
                self.start_cluster_model = RefactoredNaiveCluster(
                    loc_type='start', radius=self.radius)

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
        alg = params['alg'] if 'alg' in params.keys() else self.alg
        radius = params['radius'] if 'radius' in params.keys() else self.radius
        svm = params['svm'] if 'svm' in params.keys() else self.svm
        size_thresh = params['size_thresh'] if 'size_thresh' in params.keys(
        ) else self.size_thresh
        purity_thresh = params[
            'purity_thresh'] if 'purity_thresh' in params.keys(
            ) else self.purity_thresh
        gamma = params['gamma'] if 'gamma' in params.keys() else self.gamma
        C = params['C'] if 'C' in params.keys() else self.C
        cluster_method = params[
            'cluster_method'] if 'cluster_method' in params.keys(
            ) else self.cluster_method

        # calling __init__ again is not good practice, I know...
        self.__init__(alg, radius, svm, size_thresh, purity_thresh, gamma, C,
                      cluster_method)

        return self

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

        return self

    def predict_proba(self, test_df):
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

        # extrapolate label distributions from cluster information
        self.test_df.loc[:, [
            'mode_distrib', 'purpose_distrib', 'replaced_distrib'
        ]] = np.nan

        if self.cluster_method in ['end', 'trip']:
            cluster_col = f'{self.cluster_method}_cluster_idx'
            self.test_df = self._add_label_distributions(
                self.test_df, cluster_col)

        else:  # self.cluster_method == 'combination'
            # try to get label distributions from trip-level clusters first, because trip-level clusters tend to be more homogenous and will yield more accurate predictions
            self.test_df = self._add_label_distributions(
                self.test_df, 'trip_cluster_idx')

            # for trips that have an empty label-distribution after the first pass using trip clusters, try to get a distribution from the destination cluster (this includes both trips that *don't* fall into a trip cluster, as well as trips that *do* fall into a trip cluster but are missing some/all categories of labels due to missing user inputs.)

            # fill in missing label-distributions by the label_type
            # (we want to iterate by label_type rather than check cluster idx because it's possible that some trips in a trip-cluster have predictions for one label_type but not another)
            for label_type in ['mode', 'purpose', 'replaced']:
                self.test_df.loc[self.test_df[f'{label_type}_distrib'] ==
                                 {}] = self._add_label_distributions(
                                     self.test_df.loc[
                                         self.test_df[f'{label_type}_distrib']
                                         == {}],
                                     'end_cluster_idx',
                                     label_types=[label_type])

        # create the dataframe of probabilities
        proba_dfs = []
        for label_type in ['purpose', 'mode', 'replaced']:
            classes = self.train_df[f'{label_type}_true'].dropna().unique()
            proba = pd.DataFrame(
                self.test_df[f'{label_type}_distrib'].to_list(),
                columns=classes)
            proba['top_pred'] = proba.idxmax(axis=1)
            proba['top_proba'] = proba.max(axis=1, skipna=True)
            proba['clusterable'] = self.test_df.end_cluster_idx >= 0
            proba.loc[:, classes] = proba.loc[:, classes].fillna(0)
            proba = pd.concat([proba], keys=[label_type], axis=1)
            proba_dfs += [proba]

        self.proba_df = pd.concat(proba_dfs, axis=1)
        return self.proba_df

    def _add_label_distributions(self,
                                 df,
                                 cluster_col,
                                 label_types=['mode', 'purpose', 'replaced']):
        """ Add label distributions to a DataFrame. 

            Args: 
                df (DataFrame): DataFrame containing a column of clusters
                cluster_col (str): name of column in df containing clusters
                label_types (str list): the categories of labels to retrieve 
                    distributions for. 

            Returns:
                a DataFrame with additional columns in which the entries are 
                dictionaries containing label distributions. 
        """
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
                       f'{label_type}_distrib'] = [distrib] * cluster_size

        return df


class EnsembleClassifier(TripClassifier, metaclass=ABCMeta):
    """ Template class for trip classifiers using ensemble algorithms. 

        Required args:
            loc_feature (str): 'coordinates' or 'cluster'
    """
    base_features = [
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
    ]
    targets = ['mode_true', 'purpose_true', 'replaced_true']

    # required instance attributes
    loc_feature = NotImplemented
    purpose_enc = NotImplemented
    mode_enc = NotImplemented
    purpose_predictor = NotImplemented
    mode_predictor = NotImplemented
    replaced_predictor = NotImplemented

    # required methods
    def fit(self, train_df):
        # get location features
        if self.loc_feature == 'cluster':
            # fit clustering model(s) and one-hot encode their indices
            # TODO: consolidate start/end_cluster_model in a single instance
            # that has a location_type parameter in the fit() method
            self.end_cluster_model.fit(train_df)

            clusters_to_encode = self.end_cluster_model.train_df[[
                'end_cluster_idx'
            ]].copy()  # copy is to avoid SettingWithCopyWarning

            if self.use_start_clusters or self.use_trip_clusters:
                self.start_cluster_model.fit(train_df)

                if self.use_start_clusters:
                    clusters_to_encode = pd.concat([
                        clusters_to_encode, self.start_cluster_model.train_df[[
                            'start_cluster_idx'
                        ]]
                    ],
                                                   axis=1)
                if self.use_trip_clusters:
                    start_end_clusters = pd.concat([
                        self.end_cluster_model.train_df[['end_cluster_idx']],
                        self.start_cluster_model.train_df[[
                            'start_cluster_idx'
                        ]]
                    ],
                                                   axis=1)
                    trip_cluster_idx = self.trip_grouper.fit_transform(
                        start_end_clusters)
                    clusters_to_encode.loc[:,
                                           'trip_cluster_idx'] = trip_cluster_idx

            loc_features_df = self.cluster_enc.fit_transform(
                clusters_to_encode.astype(int))

            # clean the df again because we need it in the next step
            # TODO: remove redundancy
            self.train_df = self._clean_data(train_df)

            # TODO: move below code into a reusable function
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

        else:  # self.loc_feature == 'coordinates'
            self.train_df = self._clean_data(train_df)

            # TODO: move below code into a reusable function
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

            loc_features_df = self.train_df[[
                'start_lon', 'start_lat', 'end_lon', 'end_lat'
            ]]

        # prepare data for the ensemble classifiers

        # note that we want to use purpose data to aid our mode predictions,
        # and use both purpose and mode data to aid our replaced-mode
        # predictions
        # thus, we want to one-hot encode the purpose and mode as data
        # features, but also preserve an unencoded copy for the target columns

        # dataframe holding all features and targets
        self.Xy_train = pd.concat([
            self.train_df[self.base_features + self.targets], loc_features_df
        ],
                                  axis=1)

        # encode purposes and modes
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

        # fit classifiers
        if len(self.X_purpose) > 0:
            self.purpose_predictor.fit(self.X_purpose, self.y_purpose)
        if len(self.X_mode) > 0:
            self.mode_predictor.fit(self.X_mode, self.y_mode)
        if len(self.X_replaced) > 0:
            self.replaced_predictor.fit(self.X_replaced, self.y_replaced)

        return self

    def predict_proba(self, test_df):
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

        # TODO: some of the code across the try and except blocks can be consolidated by considering one-hot encoding fully np.nan arrays
        try:
            purpose_proba_raw = self.purpose_predictor.predict_proba(
                self.X_test_for_purpose)
            purpose_proba = pd.DataFrame(
                purpose_proba_raw, columns=self.purpose_predictor.classes_)
            purpose_pred = purpose_proba.idxmax(axis=1)

            # update X_test with one-hot-encoded purpose predictions to aid
            # mode predictor
            # TODO: converting purpose_pred to a DataFrame feels super
            # unnecessary, make this more efficient
            onehot_purpose_df = self.purpose_enc.transform(
                pd.DataFrame(purpose_pred).set_index(
                    self.X_test_for_purpose.index))
            self.X_test_for_mode = pd.concat(
                [self.X_test_for_purpose, onehot_purpose_df], axis=1)

            mode_proba, replaced_proba = self._try_predict_proba_mode_replaced(
            )

        except NotFittedError as e:
            # if we can't predict purpose, we can still try to predict mode and
            # replaced-mode without one-hot encoding the purpose

            purpose_pred = np.full((len(self.X_test_for_purpose), ), np.nan)
            purpose_proba_raw = np.full((len(self.X_test_for_purpose), 1), 0)
            purpose_proba = pd.DataFrame(purpose_proba_raw, columns=[np.nan])

            self.X_test_for_mode = self.X_test_for_purpose
            mode_proba, replaced_proba = self._try_predict_proba_mode_replaced(
            )

        mode_pred = mode_proba.idxmax(axis=1)
        replaced_pred = replaced_proba.idxmax(axis=1)

        if purpose_pred.dtype == np.float64 and mode_pred.dtype == np.float64 and replaced_pred.dtype == np.float64:
            # this indicates that all the predictions are np.nan so none of the
            # random forest classifiers were fitted
            raise NotFittedError

        # TODO: move this to a Mixin for cluster-based predictors and use the 'cluster' column of the proba_df outputs
        # if self.drop_unclustered:
        #     # TODO: actually, we should only drop purpose predictions. we can
        #     # then impute the missing entries in the purpose feature and still
        #     # try to predict mode and replaced-mode without it
        #     self.predictions.loc[
        #         self.end_cluster_model.test_df['end_cluster_idx'] == -1,
        #         ['purpose_pred', 'mode_pred', 'replaced_pred']] = np.nan

        proba_dfs = []
        for label_type, proba in zip(
            ['purpose', 'mode', 'replaced'],
            [purpose_proba, mode_proba, replaced_proba]):
            proba['top_pred'] = proba.idxmax(axis=1)
            proba['top_proba'] = proba.max(axis=1, skipna=True)
            proba['clusterable'] = self._clusterable(
                self.X_test_for_purpose).astype(bool)
            proba = pd.concat([proba], keys=[label_type], axis=1)
            proba_dfs += [proba]

        self.proba_df = pd.concat(proba_dfs, axis=1)
        return self.proba_df

    def _get_X_test_for_purpose(self, test_df):
        """ Do the pre-processing to get data that we can then pass into the 
            ensemble classifiers. 
        """
        if self.loc_feature == 'cluster':
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
            loc_features_df = self.cluster_enc.transform(clusters_to_encode)
        else:  # self.loc_feature == 'coordinates'
            test_df = self._clean_data(test_df)
            loc_features_df = test_df[[
                'start_lon', 'start_lat', 'end_lon', 'end_lat'
            ]]

        # extract the desired data
        X_test = pd.concat([
            test_df[self.base_features].reset_index(drop=True),
            loc_features_df.reset_index(drop=True)
        ],
                           axis=1)

        return X_test

    def _try_predict_proba_mode_replaced(self):
        """ Try to predict mode and replaced-mode. Handles error in case the 
            ensemble algorithms were not fitted. 
        
            Requires self.X_test_for_mode to have already been set. (These are 
            the DataFrames containing the test data to be passed into self.
            mode_predictor.) 

            Returns: mode_proba and replaced_proba, two DataFrames containing 
                class probabilities for mode and replaced-mode respectively
        """

        try:
            # predict mode
            mode_proba_raw = self.mode_predictor.predict_proba(
                self.X_test_for_mode)
            mode_proba = pd.DataFrame(mode_proba_raw,
                                      columns=self.mode_predictor.classes_)
            mode_pred = mode_proba.idxmax(axis=1)

            # update X_test with one-hot-encoded mode predictions to aid replaced-mode predictor
            onehot_mode_df = self.mode_enc.transform(
                pd.DataFrame(mode_pred).set_index(self.X_test_for_mode.index))
            self.X_test_for_replaced = pd.concat(
                [self.X_test_for_mode, onehot_mode_df], axis=1)
            replaced_proba = self._try_predict_proba_replaced()

        except NotFittedError as e:
            mode_proba_raw = np.full((len(self.X_test_for_mode), 1), 0)
            mode_proba = pd.DataFrame(mode_proba_raw, columns=[np.nan])

            # if we don't have mode predictions, we *could* still try to
            # predict replaced mode (but if the user didn't input mode labels
            # then it's unlikely they would input replaced-mode)
            self.X_test_for_replaced = self.X_test_for_mode
            replaced_proba = self._try_predict_proba_replaced()

        return mode_proba, replaced_proba

    def _try_predict_proba_replaced(self):
        """ Try to predict replaced mode. Handles error in case the 
            replaced_predictor was not fitted. 
        
            Requires self.X_test_for_replaced to have already been set. (This 
            is the DataFrame containing the test data to be passed into self.
            replaced_predictor.) 

            Returns: replaced_proba, DataFrame containing class probabilities 
                for replaced-mode
        """
        try:
            replaced_proba_raw = self.replaced_predictor.predict_proba(
                self.X_test_for_replaced
            )  # has shape (len_trips, number of replaced_mode classes)
            replaced_proba = pd.DataFrame(
                replaced_proba_raw, columns=self.replaced_predictor.classes_)

        except NotFittedError as e:
            replaced_proba_raw = np.full((len(self.X_test_for_replaced), 1), 0)
            replaced_proba = pd.DataFrame(replaced_proba_raw, columns=[np.nan])

        return replaced_proba

    def _clusterable(self, test_df):
        """ Check if the end points can be clustered (i.e. are within <radius> 
            meters of an end point from the training set) 
        """
        if self.loc_feature == 'cluster':
            return self.end_cluster_model.test_df.end_cluster_idx >= 0

        n_samples = test_df.shape[0]
        clustered = np.ones(shape=n_samples, dtype=int) * False

        train_coordinates = self.train_df[['end_lat', 'end_lon']]
        train_radians = np.radians(train_coordinates)

        for idx, row in test_df.reset_index(drop=True).iterrows():
            # calculate the distances between the ith test data and all points, then find the minimum distance for each point and check if it's within the distance threshold.
            # unfortunately, pairwise_distances_argmin() does not support haversine distance, so we have to reimplement it ourselves
            new_loc_radians = np.radians(row[["end_lat", "end_lon"]].to_list())
            new_loc_radians = np.reshape(new_loc_radians, (1, 2))
            dist_matrix_meters = haversine_distances(
                new_loc_radians, train_radians) * EARTH_RADIUS

            shortest_dist = np.min(dist_matrix_meters)
            if shortest_dist < self.radius:
                clustered[idx] = True

        return clustered


class ForestClassifier(EnsembleClassifier):
    """ Random forest-based trip classifier. 
        
        Args:
            loc_feature (str): 'coordinates' or 'cluster'; whether to use lat/
                lon coordinates or cluster indices for the location feature
            radius (int): radius for DBSCAN clustering. only if 
                loc_feature=='cluster'
            size_thresh (int): the min number of trips a cluster must have to 
                be considered for sub-division via SVM. only if 
                loc_feature=='cluster'
            purity_thresh (float): the min purity a cluster must have to be 
                sub-divided via SVM. only if loc_feature=='cluster'
            gamma (float): coefficient for the rbf kernel in SVM. only if 
                loc_feature=='cluster'
            C (float): regularization hyperparameter for SVM. only if 
                loc_feature=='cluster'
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
            use_start_clusters (bool): whether or not to use start clusters as 
                input features to the ensemble classifier. only if 
                loc_feature=='cluster'
            use_trip_clusters (bool): whether or not to use trip-level clusters 
                as input features to the ensemble classifier. only if 
                loc_feature=='cluster'
    """

    def __init__(
            self,
            loc_feature='coordinates',
            radius=100,  # TODO: add different start and end radii
            size_thresh=1,
            purity_thresh=1.0,
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
            # drop_unclustered=False,
            use_start_clusters=False,
            use_trip_clusters=True):
        assert loc_feature in ['cluster', 'coordinates']
        self.loc_feature = loc_feature
        self.radius = radius
        self.size_thresh = size_thresh
        self.purity_thresh = purity_thresh
        self.gamma = gamma
        self.C = C
        self.n_estimators = n_estimators
        self.criterion = criterion
        self.max_depth = max_depth
        self.min_samples_split = min_samples_split
        self.min_samples_leaf = min_samples_leaf
        self.max_features = max_features
        self.bootstrap = bootstrap
        self.random_state = random_state
        # self.drop_unclustered = drop_unclustered
        self.use_start_clusters = use_start_clusters
        self.use_trip_clusters = use_trip_clusters

        if self.loc_feature == 'cluster':
            # clustering algorithm to generate end clusters
            self.end_cluster_model = DBSCANSVMCluster(
                loc_type='end',
                radius=self.radius,
                size_thresh=self.size_thresh,
                purity_thresh=self.purity_thresh,
                gamma=self.gamma,
                C=self.C)

            if self.use_start_clusters or self.use_trip_clusters:
                # clustering algorithm to generate start clusters
                self.start_cluster_model = DBSCANSVMCluster(
                    loc_type='start',
                    radius=self.radius,
                    size_thresh=self.size_thresh,
                    purity_thresh=self.purity_thresh,
                    gamma=self.gamma,
                    C=self.C)

                if self.use_trip_clusters:
                    # helper class to generate trip-level clusters
                    self.trip_grouper = TripGrouper(
                        start_cluster_col='start_cluster_idx',
                        end_cluster_col='end_cluster_idx')

            # wrapper class to generate one-hot encodings for cluster indices
            self.cluster_enc = OneHotWrapper(sparse=False,
                                             handle_unknown='ignore')

        # wrapper class to generate one-hot encodings for purposes and modes
        self.purpose_enc = OneHotWrapper(impute_missing=True,
                                         sparse=False,
                                         handle_unknown='error')
        self.mode_enc = OneHotWrapper(impute_missing=True,
                                      sparse=False,
                                      handle_unknown='error')

        # ensemble classifiers for each label category
        self.purpose_predictor = RandomForestClassifier(
            n_estimators=self.n_estimators,
            criterion=self.criterion,
            max_depth=self.max_depth,
            min_samples_split=self.min_samples_split,
            min_samples_leaf=self.min_samples_leaf,
            max_features=self.max_features,
            bootstrap=self.bootstrap,
            random_state=self.random_state)
        self.mode_predictor = RandomForestClassifier(
            n_estimators=self.n_estimators,
            criterion=self.criterion,
            max_depth=self.max_depth,
            min_samples_split=self.min_samples_split,
            min_samples_leaf=self.min_samples_leaf,
            max_features=self.max_features,
            bootstrap=self.bootstrap,
            random_state=self.random_state)
        self.replaced_predictor = RandomForestClassifier(
            n_estimators=self.n_estimators,
            criterion=self.criterion,
            max_depth=self.max_depth,
            min_samples_split=self.min_samples_split,
            min_samples_leaf=self.min_samples_leaf,
            max_features=self.max_features,
            bootstrap=self.bootstrap,
            random_state=self.random_state)

    def set_params(self, params):
        """ hacky code that mimics the set_params of an sklearn Estimator class 
            so that we can pass params during randomizedsearchCV 
            
            Args:
                params (dict): a dictionary where the keys are the parameter 
                names and the values are the parameter values
        """
        loc_feature = params['loc_feature'] if 'loc_feature' in params.keys(
        ) else self.loc_feature
        radius = params['radius'] if 'radius' in params.keys() else self.radius
        size_thresh = params['size_thresh'] if 'size_thresh' in params.keys(
        ) else self.size_thresh
        purity_thresh = params[
            'purity_thresh'] if 'purity_thresh' in params.keys(
            ) else self.purity_thresh
        gamma = params['gamma'] if 'gamma' in params.keys() else self.gamma
        C = params['C'] if 'C' in params.keys() else self.C
        n_estimators = params['n_estimators'] if 'n_estimators' in params.keys(
        ) else self.n_estimators
        criterion = params['criterion'] if 'criterion' in params.keys(
        ) else self.criterion
        max_depth = params['max_depth'] if 'max_depth' in params.keys(
        ) else self.max_depth
        min_samples_split = params[
            'min_samples_split'] if 'min_samples_split' in params.keys(
            ) else self.min_samples_split
        min_samples_leaf = params[
            'min_samples_leaf'] if 'min_samples_leaf' in params.keys(
            ) else self.min_samples_leaf
        max_features = params['max_features'] if 'max_features' in params.keys(
        ) else self.max_features
        bootstrap = params['bootstrap'] if 'bootstrap' in params.keys(
        ) else self.bootstrap
        random_state = params['random_state'] if 'random_state' in params.keys(
        ) else self.random_state
        use_start_clusters = params[
            'use_start_clusters'] if 'use_start_clusters' in params.keys(
            ) else self.use_start_clusters
        # drop_unclustered = params[
        #     'drop_unclustered'] if 'drop_unclustered' in params.keys(
        #     ) else self.drop_unclustered
        use_trip_clusters = params[
            'use_trip_clusters'] if 'use_trip_clusters' in params.keys(
            ) else self.use_trip_clusters

        # yes, calling __init__ again is not good practice...
        self.__init__(loc_feature, radius, size_thresh, purity_thresh, gamma,
                      C, n_estimators, criterion, max_depth, min_samples_split,
                      min_samples_leaf, max_features, bootstrap, random_state,
                      use_start_clusters, use_trip_clusters)
        return self


class ClusterForestSlimPredictor(ForestClassifier):
    """ This is the same as ForestClassifier, just with fewer base 
        features. 

        Args:
            loc_feature (str): 'coordinates' or 'cluster'; whether to use lat/
                lon coordinates or cluster indices for the location feature
            radius (int): radius for DBSCAN clustering. only if 
                loc_feature=='cluster'
            size_thresh (int): the min number of trips a cluster must have to 
                be considered for sub-division via SVM. only if 
                loc_feature=='cluster'
            purity_thresh (float): the min purity a cluster must have to be 
                sub-divided via SVM. only if loc_feature=='cluster'
            gamma (float): coefficient for the rbf kernel in SVM. only if 
                loc_feature=='cluster'
            C (float): regularization hyperparameter for SVM. only if 
                loc_feature=='cluster'
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
            use_start_clusters (bool): whether or not to use start clusters as 
                input features to the ensemble classifier. only if 
                loc_feature=='cluster'
            use_trip_clusters (bool): whether or not to use trip-level clusters 
                as input features to the ensemble classifier. only if 
                loc_feature=='cluster'
    """

    def __init__(
            self,
            loc_feature='coordinates',
            radius=100,  # TODO: add different start and end radii
            size_thresh=1,
            purity_thresh=1.0,
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
            # drop_unclustered=False,
            use_start_clusters=False,
            use_trip_clusters=True):

        super().__init__(loc_feature, radius, size_thresh, purity_thresh,
                         gamma, C, n_estimators, criterion, max_depth,
                         min_samples_split, min_samples_leaf, max_features,
                         bootstrap, random_state, use_start_clusters,
                         use_trip_clusters)

        self.base_features = [
            'duration',
            'distance',
        ]


class AdaBoostClassifier(EnsembleClassifier):
    """ AdaBoost-based trip classifier. 

        Args:
            loc_feature (str): 'coordinates' or 'cluster'; whether to use lat/
                lon coordinates or cluster indices for the location feature
            radius (int): radius for DBSCAN clustering. only if 
                loc_feature=='cluster'
            size_thresh (int): the min number of trips a cluster must have to 
                be considered for sub-division via SVM. only if 
                loc_feature=='cluster'
            purity_thresh (float): the min purity a cluster must have to be 
                sub-divided via SVM. only if loc_feature=='cluster'
            gamma (float): coefficient for the rbf kernel in SVM. only if 
                loc_feature=='cluster'
            C (float): regularization hyperparameter for SVM. only if 
                loc_feature=='cluster'
            n_estimators (int): number of estimators
            criterion (str): function to measure the quality of a split in a decision tree
            max_depth (int): max depth of a tree in the random forest. 
                unlimited if None. 
            min_samples_split (int): min number of samples required to split an 
                internal node in a decision tree
            min_samples_leaf (int): min number of samples required for a leaf 
                node in a decision tree
            max_features (str): number of features to consider when looking for the best split in a decision tree
            random_state (int): random state for deterministic random forest 
                construction
            use_start_clusters (bool): whether or not to use start clusters as 
                input features to the ensemble classifier. only if 
                loc_feature=='cluster'
            use_trip_clusters (bool): whether or not to use trip-level clusters 
                as input features to the ensemble classifier. only if 
                loc_feature=='cluster'
            learning_rate (float): weight applied to each decision tree at each 
                boosting iteration
    """

    def __init__(
            self,
            loc_feature='coordinates',
            radius=100,  # TODO: add different start and end radii
            size_thresh=1,
            purity_thresh=1.0,
            gamma=0.05,
            C=1,
            n_estimators=100,
            criterion='gini',
            max_depth=None,
            min_samples_split=2,
            min_samples_leaf=1,
            max_features='sqrt',
            random_state=42,
            # drop_unclustered=False,
            use_start_clusters=False,
            use_trip_clusters=True,
            use_base_clusters=True,
            learning_rate=1.0):
        assert loc_feature in ['cluster', 'coordinates']
        self.loc_feature = loc_feature
        self.radius = radius
        self.size_thresh = size_thresh
        self.purity_thresh = purity_thresh
        self.gamma = gamma
        self.C = C
        self.n_estimators = n_estimators
        self.criterion = criterion
        self.max_depth = max_depth
        self.min_samples_split = min_samples_split
        self.min_samples_leaf = min_samples_leaf
        self.max_features = max_features
        self.random_state = random_state
        # self.drop_unclustered = drop_unclustered
        self.use_start_clusters = use_start_clusters
        self.use_trip_clusters = use_trip_clusters
        self.use_base_clusters = use_base_clusters
        self.learning_rate = learning_rate

        if self.loc_feature == 'cluster':
            # clustering algorithm to generate end clusters
            self.end_cluster_model = DBSCANSVMCluster(
                loc_type='end',
                radius=self.radius,
                size_thresh=self.size_thresh,
                purity_thresh=self.purity_thresh,
                gamma=self.gamma,
                C=self.C)

            if self.use_start_clusters or self.use_trip_clusters:
                # clustering algorithm to generate start clusters
                self.start_cluster_model = DBSCANSVMCluster(
                    loc_type='start',
                    radius=self.radius,
                    size_thresh=self.size_thresh,
                    purity_thresh=self.purity_thresh,
                    gamma=self.gamma,
                    C=self.C)

                if self.use_trip_clusters:
                    # helper class to generate trip-level clusters
                    self.trip_grouper = TripGrouper(
                        start_cluster_col='start_cluster_idx',
                        end_cluster_col='end_cluster_idx')

            # wrapper class to generate one-hot encodings for cluster indices
            self.cluster_enc = OneHotWrapper(sparse=False,
                                             handle_unknown='ignore')

        # wrapper class to generate one-hot encodings for purposes and modes
        self.purpose_enc = OneHotWrapper(impute_missing=True,
                                         sparse=False,
                                         handle_unknown='error')
        self.mode_enc = OneHotWrapper(impute_missing=True,
                                      sparse=False,
                                      handle_unknown='error')

        self.purpose_predictor = AdaBoostClassifier(
            n_estimators=self.n_estimators,
            learning_rate=self.learning_rate,
            random_state=self.random_state,
            base_estimator=DecisionTreeClassifier(
                criterion=self.criterion,
                max_depth=self.max_depth,
                min_samples_split=self.min_samples_split,
                min_samples_leaf=self.min_samples_leaf,
                max_features=self.max_features,
                random_state=self.random_state))
        self.mode_predictor = AdaBoostClassifier(
            n_estimators=self.n_estimators,
            learning_rate=self.learning_rate,
            random_state=self.random_state,
            base_estimator=DecisionTreeClassifier(
                criterion=self.criterion,
                max_depth=self.max_depth,
                min_samples_split=self.min_samples_split,
                min_samples_leaf=self.min_samples_leaf,
                max_features=self.max_features,
                random_state=self.random_state))
        self.replaced_predictor = AdaBoostClassifier(
            n_estimators=self.n_estimators,
            learning_rate=self.learning_rate,
            random_state=self.random_state,
            base_estimator=DecisionTreeClassifier(
                criterion=self.criterion,
                max_depth=self.max_depth,
                min_samples_split=self.min_samples_split,
                min_samples_leaf=self.min_samples_leaf,
                max_features=self.max_features,
                random_state=self.random_state))

    def set_params(self, params):
        """ hacky code that mimics the set_params of an sklearn Estimator class 
            so that we can pass params during randomizedsearchCV 
            
            Args:
                params (dict): a dictionary where the keys are the parameter 
                names and the values are the parameter values
        """
        radius = params['radius'] if 'radius' in params.keys() else self.radius
        size_thresh = params['size_thresh'] if 'size_thresh' in params.keys(
        ) else self.size_thresh
        purity_thresh = params[
            'purity_thresh'] if 'purity_thresh' in params.keys(
            ) else self.purity_thresh
        gamma = params['gamma'] if 'gamma' in params.keys() else self.gamma
        C = params['C'] if 'C' in params.keys() else self.C
        n_estimators = params['n_estimators'] if 'n_estimators' in params.keys(
        ) else self.n_estimators
        criterion = params['criterion'] if 'criterion' in params.keys(
        ) else self.criterion
        max_depth = params['max_depth'] if 'max_depth' in params.keys(
        ) else self.max_depth
        min_samples_split = params[
            'min_samples_split'] if 'min_samples_split' in params.keys(
            ) else self.min_samples_split
        min_samples_leaf = params[
            'min_samples_leaf'] if 'min_samples_leaf' in params.keys(
            ) else self.min_samples_leaf
        max_features = params['max_features'] if 'max_features' in params.keys(
        ) else self.max_features
        random_state = params['random_state'] if 'random_state' in params.keys(
        ) else self.random_state
        use_start_clusters = params[
            'use_start_clusters'] if 'use_start_clusters' in params.keys(
            ) else self.use_start_clusters
        # drop_unclustered = params[
        #     'drop_unclustered'] if 'drop_unclustered' in params.keys(
        #     ) else self.drop_unclustered
        use_trip_clusters = params[
            'use_trip_clusters'] if 'use_trip_clusters' in params.keys(
            ) else self.use_trip_clusters
        learning_rate = params[
            'learning_rate'] if 'learning_rate' in params.keys(
            ) else self.learning_rate

        # calling __init__ again is not good practice, I know...
        self.__init__(radius, size_thresh, purity_thresh, gamma, C,
                      n_estimators, criterion, max_depth, min_samples_split,
                      min_samples_leaf, max_features, random_state,
                      use_start_clusters, use_trip_clusters, learning_rate)
        return self


class TripGrouper():
    """ Helper class to get trip clusters from start and end clusters. 
    
        Args:
            start_cluster_col (str): name of the column containing start 
                cluster indices
            end_cluster_col (str): name of the column containing end cluster 
                indices
    """

    def __init__(self,
                 start_cluster_col='start_cluster_idx',
                 end_cluster_col='end_cluster_idx'):
        self.start_cluster_col = start_cluster_col
        self.end_cluster_col = end_cluster_col

    def fit_transform(self, trip_df):
        """ Fit and remember possible trip clusters. 
        
            Args:
                trip_df (DataFrame): DataFrame containing trips. must have 
                    columns <start_cluster_col> and <end_cluster_col>
        """
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
        """ Get trip clusters for a new set of trips. 
        
            Args:
                new_trip_df (DataFrame): DataFrame containing trips. must have 
                    columns <start_cluster_col> and <end_cluster_col>
        """
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
    """ Helper class to streamline one-hot encoding. 
    
        Args: 
            impute_missing (bool): whether or not to impute np.nan values. 
            sparse (bool): whether or not to return a sparse matrix. 
            handle_unknown (str): specifies the way unknown categories are 
                handled during transform.
    """

    def __init__(
        self,
        impute_missing=False,
        sparse=False,
        handle_unknown='ignore',
    ):
        self.impute_missing = impute_missing
        if self.impute_missing:
            self.encoder = make_pipeline(
                SimpleImputer(missing_values=np.nan,
                              strategy='constant',
                              fill_value='missing'),
                OneHotEncoder(sparse=False, handle_unknown=handle_unknown))
        else:
            self.encoder = OneHotEncoder(sparse=sparse,
                                         handle_unknown=handle_unknown)

    def fit_transform(self, train_df, output_col_prefix=None):
        """ Establish one-hot encoded variables. 
        
            Args: 
                train_df (DataFrame): DataFrame containing train trips. 
                output_col_prefix (str): only if train_df is a single column
        """
        # TODO: handle pd series

        train_df = train_df.copy()  # to avoid SettingWithCopyWarning

        # if imputing, the dtype of each column must be string/object and not numerical, otherwise the SimpleImputer will fail
        if self.impute_missing:
            for col in train_df.columns:
                train_df[col] = train_df[col].astype(object)
        onehot_encoding = self.encoder.fit_transform(train_df)
        self.onehot_encoding_cols_all = []
        for col in train_df.columns:
            if train_df.shape[1] > 1 or output_col_prefix is None:
                output_col_prefix = col
            self.onehot_encoding_cols_all += [
                f'{output_col_prefix}_{val}'
                for val in np.sort(train_df[col].dropna().unique())
            ]
            # we handle np.nan separately because it is of type float, and may cause issues with np.sort if the rest of the unique values are strings
            if any((train_df[col].isna())):
                self.onehot_encoding_cols_all += [f'{output_col_prefix}_nan']

        onehot_encoding_df = pd.DataFrame(
            onehot_encoding,
            columns=self.onehot_encoding_cols_all).set_index(train_df.index)

        # ignore the encoded columns for missing entries
        self.onehot_encoding_cols = copy.deepcopy(
            self.onehot_encoding_cols_all)
        for col in self.onehot_encoding_cols_all:
            if col.endswith('_nan'):
                onehot_encoding_df = onehot_encoding_df.drop(columns=[col])
                self.onehot_encoding_cols.remove(col)

        return onehot_encoding_df.astype(int)

    def transform(self, test_df):
        """ One-hot encoded features in accordance with features seen in the 
            train set. 
        
            Args: 
                test_df (DataFrame): DataFrame of trips. 
        """
        # TODO: rename test_df, this one doesn't necessarily need to be a df
        onehot_encoding = self.encoder.transform(test_df)
        onehot_encoding_df = pd.DataFrame(
            onehot_encoding,
            columns=self.onehot_encoding_cols_all).set_index(test_df.index)

        # ignore the encoded columns for missing entries
        for col in self.onehot_encoding_cols_all:
            if col.endswith('_nan'):
                onehot_encoding_df = onehot_encoding_df.drop(columns=[col])

        return onehot_encoding_df.astype(int)