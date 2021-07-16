import logging
import scipy.cluster.hierarchy as sch
import sklearn.cluster as sc

# to map the user labels
# - user_input_df: pass in original user input dataframe, return changed user input dataframe
# - sp2en: change Spanish to English
def map_labels_sp2en(user_input_df):
    # Spanish words to English
    span_eng_dict = {'revisado_bike': 'test ride with bike', 'placas_de carro': 'car plates', 'aseguranza': 'insurance',
                     'iglesia': 'church', 'curso': 'course',
                     'mi_hija recién aliviada': 'my daughter just had a new baby',
                     'servicio_comunitario': 'community service', 'pago_de aseguranza': 'insurance payment',
                     'grupo_comunitario': 'community group', 'caminata_comunitaria': 'community walk'}

    # change language
    user_input_df = user_input_df.replace(span_eng_dict)
    return user_input_df


# to map purposes and replaced mode in user inputs
# - cvt_pur_mo: convert purposes and replaced mode
def map_labels_purpose(user_input_df):
    # Convert purpose
    map_pur_dict = {'course': 'school', 'work_- lunch break': 'lunch_break', 'on_the way home': 'home',
                    'insurance_payment': 'insurance'}

    # convert purpose
    user_input_df = user_input_df.replace(map_pur_dict)
    return user_input_df


def map_labels_mode(user_input_df):
    # convert mode
    for a in range(len(user_input_df)):
        if user_input_df.iloc[a]["replaced_mode"] == "same_mode":
            # to see which row will be converted
            logging.debug("The following rows will be changed: %s", user_input_df.iloc[a])
            user_input_df.iloc[a]["replaced_mode"] = user_input_df.iloc[a]['mode_confirm']
    return user_input_df


# this function will change Spanish to English, convert purposes, and convert modes
def map_labels(user_input_df):
    # Note that the spanish -> english conversion MUST currently happen before the other
    # mode and purpose mappings
    user_input_df = map_labels_sp2en(user_input_df)
    user_input_df = map_labels_purpose(user_input_df)
    user_input_df = map_labels_mode(user_input_df)
    return user_input_df

# use hierarchical clustering to get labels of the second round
# - sch.linkage: perform hierarchical(agglomerative) clustering
# In this function, we set a low bound and a higher bound(cutoff) of distance in the dendrogram
# - last_d: the distance of the last cluster in the dendrogram
# - low: the lower bound of distance
# e.g., if low = 300, last_d = 250, we will assign 0s as labels for the points, irrespective of the first round labels.
# and the list of second round labels will be like [0,0,0,0,0].
# It means the points are already similar to each other after the first round of clustering, they don't need to
# go through the second round.
# - max_d: the cutoff of distance
# - dist_pct: the percentage of the last distance in the dendrogram
# - sch.fcluster: form clusters from the hierarchical clustering defined by the given linkage matrix
# e.g., if last_d = 10000, dist_pct = 0.4, max_d = 400, clusters will be assigned at the distance of 400
# by default, using scipy hierarchical clustering
def get_second_labels(x,method,low,dist_pct):
    z = sch.linkage(x, method=method, metric='euclidean')
    last_d = z[-1][2]
    clusters = []
    if last_d < low:
        for i in range(len(x)):
            clusters.append(0)
    else:
        max_d = last_d * dist_pct
        clusters = sch.fcluster(z, max_d, criterion='distance')
    return clusters

# using kmeans to build the model
def kmeans_clusters(clusters,x):
    n_clusters = len(set(clusters))
    kmeans = sc.KMeans(n_clusters=n_clusters, random_state=0).fit(x)
    k_clusters = kmeans.labels_
    return k_clusters


# this function includes hierarchical clustering and changing labels from the first round to get appropriate labels for
# the second round of clustering
# appropriate labels are label from the first round concatenate label from the second round
# (e.g. label from first round is 1, label from second round is 2, the new label will be 12)
# - second_round_idx_labels: a list to store the indices and labels from the first round.
# - second_labels: labels from the second round of clustering
def get_new_labels(second_labels,second_round_idx_labels,new_labels):
    for i in range(len(second_labels)):
        first_index = second_round_idx_labels[i][0]
        new_label = second_round_idx_labels[i][1]
        # concatenate labels from two rounds
        new_label = int(str(new_label) + str(second_labels[i]))
        for k in range(len(new_labels)):
            if k == first_index:
                new_labels[k] = new_label
                break
    return new_labels


# group similar trips according to new_labels, store the original indices of the trips
def group_similar_trips(new_labels,track):
    bin_sim_trips_idx = []

    # find the unique set of bins and store their indices into `bin_sim_trips`
    label_set = set(new_labels)
    # convert the set of unique labels into their indices
    # concretely, if the input labels are ['a','a','a','b','b','b']
    # the unique labels are ['a', 'b']
    for sel_label in label_set:
        # for the first iteration, bin = [0,1,2]
        # for the second iteration, bin = [3,4,5]
        bin = [index for (index, label) in enumerate(new_labels) if label == sel_label]
        bin_sim_trips_idx.append(bin)
    # At the end, bin_sim_trips_idx = [[0,1,2],[3,4,5]]

    # using track to replace the current indices with original indices
    for bin in bin_sim_trips_idx:
        # in the first iteration, bin = [0,1,2]
        # in the first iteration of that, we map the trip index of the
        # common trip (e.g. 0) to the original index for that trip from the track (e.g. 42)
        for i in range(len(bin)):
            bin[i] = track[bin[i]][0]
    # At this point, the bin_sim_trips_idx will have original indices for the trips
    return bin_sim_trips_idx



# replace the first round labels with new labels
# - track: a list to store the indices and labels from the first round of clustering
# for item in track, item[0] is the original index of the trip in filter_trips
# item[1] is the label after the first round of clustering
# we change the labels from the first round with new labels from the second round here
def change_track_labels(track,new_labels):
    for i in range(len(new_labels)):
        track[i][1] = new_labels[i]
    return track
