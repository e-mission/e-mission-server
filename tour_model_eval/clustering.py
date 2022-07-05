# helper functions to streamline the use and comparison of clustering algs

# basic imports
import pandas as pd
import numpy as np
import logging

# import clustering algorithms
import sklearn.metrics.pairwise as smp
import sklearn.cluster as sc
from sklearn import svm
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

# our imports
import emission.analysis.modelling.tour_model_extended.similarity as eamts
import emission.storage.decorations.trip_queries as esdtq

EARTH_RADIUS = 6371000


def add_loc_clusters(loc_df,
                     radii,
                     alg,
                     loc_type,
                     min_samples=2,
                     optics_min_samples=None,
                     optics_xi=0.05,
                     optics_cluster_method='xi',
                     svm_min_size=6,
                     purity_thresh=0.7,
                     svm_gamma=0.05,
                     svm_C=1,
                     purpose_col='purpose_confirm'):
    """ Given a dataframe of trips, cluster the locations (either start or end 
        locations) using the desired algorithm and append columns with the 
        resulting cluster indices
        
        Args:
            loc_df (dataframe): must have columns 'start_lat' and 'start_lon' 
                or 'end_lat' and 'end_lon'
            radii (int list): list of radii to run the clustering algs with
            alg (str): 'DBSCAN', 'oursim', 'OPTICS', 'SVM', 'fuzzy'
            loc_type (str): 'start' or 'end'
            min_samples (int): min samples per cluster. used in DBSCAN (and 
                therefore also SVM and fuzzy, for now)
            optics_min_samples (int): min samples per cluster, if using OPTICS.
            optics_xi (float): xi value if using the xi method of OPTICS.
            optics_cluster_method (str): method to use for the OPTICS 
                algorithm. either 'xi' or 'dbscan'
            svm_min_size (int): if using SVM, the min number of trips in a 
                cluster that can be sub-divided
            purity_thresh (float): if using SVM, the min purity of a cluster 
                below which the cluster will be sub-divided
            svm_gamma (float): if using SVM, the gamma hyperparameter
            svm_C(float): if using SVM, the C hyperparameter
            purpose_col (str): name of the column containing trip purpose
    """
    assert loc_type == 'start' or loc_type == 'end'

    alg_options = ['DBSCAN', 'oursim', 'OPTICS', 'SVM', 'fuzzy']
    assert alg in alg_options

    # if using SVM, we get the initial clusters with DBSCAN, then sub-divide
    if alg == 'DBSCAN' or alg == 'SVM':
        dist_matrix_meters = get_distance_matrix(loc_df, loc_type)

        for r in radii:
            labels = sc.DBSCAN(
                r, metric="precomputed",
                min_samples=min_samples).fit(dist_matrix_meters).labels_

            # pd.Categorical converts the type from int to category (so
            # numerical operations aren't possible)
            # loc_df.loc[:,
            #            f"{loc_type}_DBSCAN_clusters_{r}_m"] = pd.Categorical(
            #                labels)
            # TODO: fix this and make it Categorical again (right now labels are
            # ints)
            loc_df.loc[:, f"{loc_type}_DBSCAN_clusters_{r}_m"] = labels

            # move "noisy" trips to their own single-trip clusters
            for idx in loc_df.loc[loc_df[f"{loc_type}_DBSCAN_clusters_{r}_m"]
                                  == -1].index.values:
                loc_df.loc[idx,
                           f"{loc_type}_DBSCAN_clusters_{r}_m"] = 1 + loc_df[
                               f"{loc_type}_DBSCAN_clusters_{r}_m"].max()

        # TODO: this needs to be updated so that we can actually generate
        # predictions for unlabeled trips
        if alg == 'SVM':
            for r in radii:
                # copy DBSCAN clusters as SVM clusters
                loc_df.loc[:,
                           f"{loc_type}_{alg}_clusters_{r}_m"] = loc_df.loc[:,
                                                                            f"{loc_type}_DBSCAN_clusters_{r}_m"]

                c = 0
                # iterate over all clusters using a while loop so that we can
                # do multiple iterations of SVM sub-dividing if needed
                while c < loc_df[f"{loc_type}_{alg}_clusters_{r}_m"].max():
                    points_in_cluster = loc_df.loc[
                        loc_df[f"{loc_type}_{alg}_clusters_{r}_m"] == c]

                    # only do SVM if we have at least 6 points in the cluster
                    # (or custom min_size)
                    if len(points_in_cluster) < svm_min_size:
                        c += 1
                        continue

                    # only do SVM if purity is below threshold
                    purity = get_purity(points_in_cluster)
                    if purity < purity_thresh:
                        X = points_in_cluster[[
                            f"{loc_type}_lon", f"{loc_type}_lat"
                        ]]
                        y = points_in_cluster[purpose_col].to_list()

                        svc = make_pipeline(
                            StandardScaler(),
                            svm.SVC(
                                kernel='rbf',
                                gamma=svm_gamma,
                                C=svm_C,
                            ))

                        svc.fit(X, y)

                        # map from purpose labels to new cluster indices
                        # we offset indices by the max existing index so that
                        # we don't run into any duplicate trouble
                        max_existing_idx = np.amax(
                            loc_df[f"{loc_type}_{alg}_clusters_{r}_m"].unique(
                            ))

                        # # if the indices are Categorical, need to convert to
                        # # ordered values
                        # max_existing_idx = np.amax(
                        #     existing_cluster_indices.as_ordered())

                        labels = np.array(svc.predict(X))
                        unique_labels = np.unique(labels)
                        label_to_cluster = {
                            unique_labels[i]: i + max_existing_idx + 1
                            for i in range(len(unique_labels))
                        }

                        # if the SVM predicts everything with the same label,
                        # just ignore it and don't reindex.
                        # this also helps us to handle the possibility that a
                        # cluster may be impure but inherently inseparable, e.g.
                        # a cluster containing 'home' trips and round trips to pick up/drop off. we don't want to reindex otherwise the low purity will trigger SVM again, and we will
                        # attempt & fail to split the cluster, ad infinitum
                        if len(unique_labels) > 1:
                            indices = np.array(
                                [label_to_cluster[l] for l in labels])

                            loc_df.loc[
                                loc_df[f"{loc_type}_{alg}_clusters_{r}_m"] ==
                                c,
                                f"{loc_type}_{alg}_clusters_{r}_m"] = indices

                    c += 1

    elif alg == 'oursim':
        for r in radii:
            # this is using a modified Similarity class that bins start/end
            # points separately before creating trip-level bins
            sim_model = eamts.Similarity(loc_df,
                                         radius_start=r,
                                         radius_end=r,
                                         shouldFilter=False,
                                         cutoff=False)
            sim_model.fit()
            labels = sim_model.data_df[loc_type + '_bin']

            # pd.Categorical converts the type from int to category (so
            # numerical operations aren't possible)
            loc_df.loc[:, f"{loc_type}_{alg}_clusters_{r}_m"] = pd.Categorical(
                labels)

    elif alg == 'OPTICS':
        if optics_min_samples == None:
            optics_min_samples = 2
        dist_matrix_meters = get_distance_matrix(loc_df, loc_type)

        for r in radii:
            labels = sc.OPTICS(
                min_samples=optics_min_samples,
                max_eps=r,
                xi=optics_xi,
                cluster_method=optics_cluster_method,
                metric="precomputed").fit(dist_matrix_meters).labels_

            # pd.Categorical converts the type from int to category (so
            # numerical operations aren't possible)
            loc_df.loc[:, f"{loc_type}_{alg}_clusters_{r}_m"] = pd.Categorical(
                labels)

    elif alg == 'fuzzy':
        # create clusters with completely homogeneous purpose labels
        # I'm calling this 'fuzzy' for now since the clusters overlap, but I
        # need to think of a better name
        purpose_list = loc_df.purpose_confirm.dropna().unique()

        for p in purpose_list:
            p_loc_df = loc_df.loc[loc_df['purpose_confirm'] == p]
            dist_matrix_meters = get_distance_matrix(p_loc_df, loc_type)

            for r in radii:
                labels = sc.DBSCAN(
                    r, metric="precomputed",
                    min_samples=min_samples).fit(dist_matrix_meters).labels_

                # pd.Categorical converts the type from int to category (so
                # numerical operations aren't possible)
                # loc_df.loc[:,
                #            f"{loc_type}_DBSCAN_clusters_{r}_m"] = pd.Categorical(
                #                labels)
                loc_df.loc[loc_df['purpose_confirm'] == p,
                           f"{loc_type}_{alg}_clusters_{r}_m"] = labels

                # move "noisy" trips to their own single-trip clusters
                noisy_trips = loc_df.loc[(loc_df['purpose_confirm'] == p) & (
                    loc_df[f"{loc_type}_{alg}_clusters_{r}_m"] == -1)]
                for idx in noisy_trips.index.values:
                    max_idx_inside_p = loc_df.loc[
                        (loc_df['purpose_confirm'] == p),
                        f"{loc_type}_{alg}_clusters_{r}_m"].max()
                    loc_df.loc[
                        idx,
                        f"{loc_type}_{alg}_clusters_{r}_m"] = 1 + max_idx_inside_p

                # we offset all cluster indices for purpose p by the max
                # existing index excluding purpose p
                # so that we don't run into any duplicate trouble
                max_idx_outside_p = loc_df.loc[
                    (loc_df['purpose_confirm'] != p),
                    f"{loc_type}_{alg}_clusters_{r}_m"].max(skipna=True)

                if np.isnan(max_idx_outside_p):
                    # can happen if column is empty, e.g. if this is the first
                    # purpose in the list that we are iterating over
                    max_idx_outside_p = -1

                logging.debug('max_idx_outside_p', max_idx_outside_p,
                              "at radius", r)

                loc_df.loc[
                    loc_df['purpose_confirm'] == p,
                    f"{loc_type}_{alg}_clusters_{r}_m"] += 1 + max_idx_outside_p

    return loc_df


def get_distance_matrix(loc_df, loc_type):
    """ Args:
            loc_df (dataframe): must have columns 'start_lat' and 'start_lon' 
                or 'end_lat' and 'end_lon'
            loc_type (str): 'start' or 'end'
    """
    assert loc_type == 'start' or loc_type == 'end'
    logging.debug('in get_distance_matrix')
    radians_lat_lon = np.radians(loc_df[[loc_type + "_lat",
                                         loc_type + "_lon"]])

    dist_matrix_meters = pd.DataFrame(
        smp.haversine_distances(radians_lat_lon, radians_lat_lon) *
        EARTH_RADIUS)
    return dist_matrix_meters


def get_purity(points_in_cluster, label_column="purpose_confirm"):
    """ Calculates purity of a cluster (i.e. % of trips that have the most 
        common label)
    
        Args:
            points_in_cluster (df): dataframe who's rows are in the same cluster
            label_column (str): name of the column containing labels
    """
    most_freq_label = points_in_cluster[label_column].mode()[0]
    purity = len(points_in_cluster[points_in_cluster[label_column] ==
                                   most_freq_label]) / len(points_in_cluster)
    return purity
