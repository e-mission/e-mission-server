import emission.analysis.modelling.trip_model.trip_model as eamuu
from sklearn.cluster import DBSCAN
import logging 
import numpy as np
import pandas as pd
import emission.analysis.modelling.trip_model.util as eamtu
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from sklearn import svm
from sklearn.metrics.pairwise import haversine_distances

EARTH_RADIUS = 6371000

class DBSCANSVMCluster(eamuu.TripModel):
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
        logging.info("PERF: Initializing DBSCANSVMCluster")
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

    def fit(self, train_df,ct_entry=None):
        """ Creates clusters of trip points. 
            self.train_df will be updated with columns containing base and 
            final clusters. 

            TODO: perhaps move the loc_type argument to fit() so we can use a 
            single class instance to cluster both start and end points. This 
            will also help us reduce duplicate data. 

            Args:
                train_df (dataframe): dataframe of labeled trips
                ct_entry (List) : A list of Entry type of labeled and unlabeled trips 
        """
        ##################
        ### clean data ###
        ##################
        logging.info("PERF: Fitting DBSCANSVMCluster")
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
        dist_matrix_meters = eamtu.get_distance_matrix(self.train_df, self.loc_type)
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
                purity = eamtu.single_cluster_purity(points_in_cluster,
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
        logging.info("PERF: Predicting DBSCANSVMCluster")
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
        logging.info("PERF: NN_predicting DBSCANSVMCluster")
        n_samples = test_df.shape[0]
        labels = np.ones(shape=n_samples, dtype=int) * -1

        # get coordinates of core points (we can't use model.components_
        # because our input feature was a distance matrix and doesn't contain
        # info about the raw coordinates)
        # NOTE: technically, every single point in a cluster is a core point
        # because it has at least minPts (2) points, including itself, in its
        # radius
        train_coordinates = self.train_df[[
            f'{self.loc_type}_lat', f'{self.loc_type}_lon'
        ]]
        train_radians = np.radians(train_coordinates)

        for idx, row in test_df.reset_index(drop=True).iterrows():
            # calculate the distances between the ith test data and all points,
            # then find the index of the closest point. if the ith test data is
            # within epsilon of the point, then assign its cluster to the ith
            # test data (otherwise, leave it as -1, indicating noise).
            # unfortunately, pairwise_distances_argmin() does not support
            # haversine distance, so we have to reimplement it ourselves
            new_loc_radians = np.radians(
                row[[self.loc_type + "_lat", self.loc_type + "_lon"]].to_list())
            new_loc_radians = np.reshape(new_loc_radians, (1, 2))
            dist_matrix_meters = haversine_distances(
                new_loc_radians, train_radians) * EARTH_RADIUS

            shortest_dist_idx = np.argmin(dist_matrix_meters)
            if dist_matrix_meters[0, shortest_dist_idx] < self.radius:
                labels[idx] = self.train_df.reset_index(
                    drop=True).loc[shortest_dist_idx,
                                   f'{self.loc_type}_cluster_idx']

        return labels
    
