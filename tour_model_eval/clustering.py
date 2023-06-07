# helper functions to streamline the use and comparison of clustering algs

# basic imports
import pandas as pd
import numpy as np
import logging

# import clustering algorithms
import sklearn.metrics.pairwise as smp
import sklearn.cluster as sc
from sklearn import metrics
from sklearn import svm
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

# our imports
# NOTE: this requires changing the branch of e-mission-server to
# eval-private-data-compatibility
import emission.analysis.modelling.tour_model_extended.similarity as eamts
import emission.storage.decorations.trip_queries as esdtq

EARTH_RADIUS = 6371000
ALG_OPTIONS = [
    'DBSCAN',
    'naive',
    'OPTICS',
    # 'fuzzy',
    'mean_shift'
]


def add_loc_clusters(
        loc_df,
        radii,
        loc_type,
        alg,
        SVM=False,
        #  cluster_unlabeled=False,
        min_samples=1,
        optics_min_samples=None,
        optics_xi=0.05,
        optics_cluster_method='xi',
        svm_min_size=6,
        svm_purity_thresh=0.7,
        svm_gamma=0.05,
        svm_C=1):
    """ Given a dataframe of trips, cluster the locations (either start or end 
        locations) using the desired algorithm & parameters.
        
        Returns: 
            Same dataframe, with appended columns that contain the resulting cluster indices. 
        
        Args:
            loc_df (dataframe): must have columns 'start_lat' and 'start_lon' 
                or 'end_lat' and 'end_lon'
            radii (int list): list of radii to run the clustering algs with
            loc_type (str): 'start' or 'end'
            alg (str): 'DBSCAN', 'naive', 'OPTICS', 'SVM', 'fuzzy', or
                'mean_shift'
            SVM (bool): whether or not to sub-divide clusters with SVM
            # cluster_unlabeled (bool): whether or not unlabeled points are used 
            #     to generate clusters.
            min_samples (int): min samples per cluster. used in DBSCAN (and 
                therefore also SVM and fuzzy, for now)
            optics_min_samples (int): min samples per cluster, if using OPTICS.
            optics_xi (float): xi value if using the xi method of OPTICS.
            optics_cluster_method (str): method to use for the OPTICS 
                algorithm. either 'xi' or 'dbscan'
            svm_min_size (int): the min number of trips a cluster must have to 
                be considered for sub-division, if using SVM
            svm_purity_thresh (float): the min purity a cluster must have to be 
                sub-divided, if using SVM
            svm_gamma (float): if using SVM, the gamma hyperparameter
            svm_C (float): if using SVM, the C hyperparameter
    """
    assert loc_type == 'start' or loc_type == 'end'
    assert alg in ALG_OPTIONS

    # if using SVM, we get the initial clusters with DBSCAN, then sub-divide
    if alg == 'DBSCAN':
        dist_matrix_meters = get_distance_matrix(loc_df, loc_type)

        for r in radii:
            model = sc.DBSCAN(r, metric="precomputed",
                              min_samples=min_samples).fit(dist_matrix_meters)
            labels = model.labels_
            # print(model.n_features_in_)
            # print(model.components_.shape)
            # print(model.components_)

            # pd.Categorical converts the type from int to category (so
            # numerical operations aren't possible)
            # loc_df.loc[:,
            #            f"{loc_type}_DBSCAN_clusters_{r}_m"] = pd.Categorical(
            #                labels)
            # TODO: fix this and make it Categorical again (right now labels are
            # ints)
            loc_df.loc[:, f"{loc_type}_DBSCAN_clusters_{r}_m"] = labels

    elif alg == 'naive':
        for r in radii:
            # this is using a modified Similarity class that bins start/end
            # points separately before creating trip-level bins
            sim_model = eamts.Similarity(loc_df,
                                         radius_start=r,
                                         radius_end=r,
                                         shouldFilter=False,
                                         cutoff=False)
            # we only bin the loc_type points to speed up the alg. avoid
            # unnecessary binning since this is really slow
            sim_model.bin_helper(loc_type=loc_type)
            labels = sim_model.data_df[loc_type + '_bin'].to_list()

            # # pd.Categorical converts the type from int to category (so
            # # numerical operations aren't possible)
            # loc_df.loc[:, f"{loc_type}_{alg}_clusters_{r}_m"] = pd.Categorical(
            #     labels)
            loc_df.loc[:, f"{loc_type}_{alg}_clusters_{r}_m"] = labels

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

            # # pd.Categorical converts the type from int to category (so
            # # numerical operations aren't possible)
            # loc_df.loc[:, f"{loc_type}_{alg}_clusters_{r}_m"] = pd.Categorical(
            #     labels)
            loc_df.loc[:, f"{loc_type}_{alg}_clusters_{r}_m"] = labels

    elif alg == 'fuzzy':
        # create clusters with completely homogeneous purpose labels
        # I'm calling this 'fuzzy' for now since the clusters overlap, but I
        # need to think of a better name
        logging.warning(
            'This alg is not properly implemented and will not generate clusters for unlabeled trips!'
        )

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

    elif alg == 'mean_shift':
        for r in radii:
            # seems like the bandwidth is based on the raw lat/lon data (we
            # never pass in a distance matrix), so we want a conversion factor
            # from meters to degrees. Since 100-500m corresponds to such a
            # small degree change, we can rely on the small angle approximation
            # and just use a linear multiplier. This conversion factor doesn't
            # have to be *super* accurate, its just so we can get a sense of
            # what the bandwidth roughly corresponds to in the real world/make
            # the value a little more interpretable.
            LATLON_TO_M = 1 / 111139
            labels = sc.MeanShift(
                bandwidth=LATLON_TO_M * r,
                min_bin_freq=min_samples,
                cluster_all=False,
            ).fit(loc_df[[f"{loc_type}_lon", f"{loc_type}_lat"]]).labels_

            # pd.Categorical converts the type from int to category (so
            # numerical operations aren't possible)
            # loc_df.loc[:,
            #            f"{loc_type}_DBSCAN_clusters_{r}_m"] = pd.Categorical(
            #                labels)
            # TODO: fix this and make it Categorical again (right now labels are
            # ints)
            loc_df.loc[:, f"{loc_type}_mean_shift_clusters_{r}_m"] = labels

            # move "noisy" trips to their own single-trip clusters
            for idx in loc_df.loc[
                    loc_df[f"{loc_type}_mean_shift_clusters_{r}_m"] ==
                    -1].index.values:
                loc_df.loc[
                    idx, f"{loc_type}_mean_shift_clusters_{r}_m"] = 1 + loc_df[
                        f"{loc_type}_mean_shift_clusters_{r}_m"].max()

    if SVM:
        loc_df = add_loc_SVM(loc_df, radii, alg, loc_type, svm_min_size,
                             svm_purity_thresh, svm_gamma, svm_C)
    return loc_df


def add_loc_SVM(loc_df,
                radii,
                alg,
                loc_type,
                svm_min_size=6,
                svm_purity_thresh=0.7,
                svm_gamma=0.05,
                svm_C=1,
                cluster_cols=None):
    """ Sub-divide base clusters using SVM.
        
        Args:
            loc_df (dataframe): must have columns 'start_lat' and 'start_lon' 
                or 'end_lat' and 'end_lon', as well as 
                '{loc_type}_{base_alg}_SVM_clusters_{r}_m', containing cluster indices generated by the base clustering alg
            radii (int list): list of radii to run the clustering algs with
            loc_type (str): 'start' or 'end'
            svm_min_size (int): the min number of trips a cluster must have to 
                be considered for sub-division
            svm_purity_thresh (float): the min purity a cluster must have to be 
                sub-divided
            svm_gamma (float): the gamma hyperparameter
            svm_C(float): the C hyperparameter
            cluster_col (str list): names of column containing cluster indices 
                of interest
    """
    assert loc_type == 'start' or loc_type == 'end'
    assert f'{loc_type}_lat' in loc_df.columns
    assert f'{loc_type}_lon' in loc_df.columns

    for i in range(len(radii)):
        r = radii[i]
        if cluster_cols == None:
            cluster_col = f"{loc_type}_{alg}_clusters_{r}_m"
        else:
            cluster_col = cluster_cols[i]
        assert cluster_col in loc_df.columns

        # c is the count of how many clusters we have iterated over
        c = 0
        # iterate over all clusters and subdivide them with SVM. The while loop
        # is so we can do multiple iterations of subdividing if needed
        while c < loc_df[cluster_col].max():
            points_in_cluster = loc_df.loc[loc_df[cluster_col] == c]

            labeled_points_in_cluster = points_in_cluster.dropna(
                subset=['purpose_confirm'])

            # only do SVM if we have at least labeled 6 points in the cluster
            # (or custom min_size)
            if len(labeled_points_in_cluster) < svm_min_size:
                c += 1
                continue

            # only do SVM if purity is below threshold
            purity = single_cluster_purity(labeled_points_in_cluster)
            if purity < svm_purity_thresh:
                X_train = labeled_points_in_cluster[[
                    f"{loc_type}_lon", f"{loc_type}_lat"
                ]]
                X_all = points_in_cluster[[
                    f"{loc_type}_lon", f"{loc_type}_lat"
                ]]
                y_train = labeled_points_in_cluster.purpose_confirm.to_list()

                labels = make_pipeline(
                    StandardScaler(),
                    svm.SVC(
                        kernel='rbf',
                        gamma=svm_gamma,
                        C=svm_C,
                    )).fit(X_train, y_train).predict(X_all)

                unique_labels = np.unique(labels)

                # map from purpose labels to new cluster indices
                # we offset indices by the max existing index so that
                # we don't run into any duplicate trouble
                max_existing_idx = loc_df[cluster_col].max()

                # # if the indices are Categorical, need to convert to
                # # ordered values
                # max_existing_idx = np.amax(
                #     existing_cluster_indices.as_ordered())

                # labels = np.array(svc.predict(X))
                label_to_cluster = {
                    unique_labels[i]: i + max_existing_idx + 1
                    for i in range(len(unique_labels))
                }

                # if the SVM predicts everything with the same label, just
                # ignore it and don't reindex.

                # this also helps us to handle the possibility that a cluster
                # may be impure but inherently inseparable, e.g. an end cluster
                # containing 50% 'home' trips and 50% round trips to pick up/
                # drop off. we don't want to reindex otherwise the low purity
                # will trigger SVM again, and we will attempt & fail to split
                # the cluster ad infinitum
                if len(unique_labels) > 1:
                    indices = np.array([label_to_cluster[l] for l in labels])

                    loc_df.loc[loc_df[cluster_col] == c, cluster_col] = indices

            c += 1

    return loc_df


def get_distance_matrix(loc_df, loc_type):
    """ Args:
            loc_df (dataframe): must have columns 'start_lat' and 'start_lon' 
                or 'end_lat' and 'end_lon'
            loc_type (str): 'start' or 'end'
    """
    assert loc_type == 'start' or loc_type == 'end'

    radians_lat_lon = np.radians(loc_df[[loc_type + "_lat", loc_type + "_lon"]])

    dist_matrix_meters = pd.DataFrame(
        smp.haversine_distances(radians_lat_lon, radians_lat_lon) *
        EARTH_RADIUS)
    return dist_matrix_meters


def single_cluster_purity(points_in_cluster, label_col='purpose_confirm'):
    """ Calculates purity of a cluster (i.e. % of trips that have the most 
        common label)
    
        Args:
            points_in_cluster (df): dataframe containing points in the same 
                cluster
            label_col (str): column in the dataframe containing labels
    """
    assert label_col in points_in_cluster.columns

    most_freq_label = points_in_cluster[label_col].mode()[0]
    purity = len(points_in_cluster[points_in_cluster[label_col] ==
                                   most_freq_label]) / len(points_in_cluster)
    return purity


def purity_score(y_true, y_pred):
    contingency_matrix = metrics.cluster.contingency_matrix(y_true, y_pred)
    purity = np.sum(np.amax(contingency_matrix,
                            axis=0)) / np.sum(contingency_matrix)
    return purity
