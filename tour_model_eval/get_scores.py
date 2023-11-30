import pandas as pd
import pandas.testing as pdt
import label_processing as label_pro
import sklearn.metrics as skm
import itertools


# compare the trip orders in bin_trips with those in filter_trips above cutoff
def compare_trip_orders(bins,bin_trips,filter_trips):
    bin_trips_ts = pd.DataFrame(data=[trip["data"]["start_ts"] for trip in bin_trips])
    bin_ls = list(itertools.chain(*bins))
    bins_ts = pd.DataFrame(data=[filter_trips[i]["data"]["start_ts"] for i in bin_ls])
    # compare two data frames, the program will continue to score calculation if two data frames are the same
    pdt.assert_frame_equal(bins_ts, bin_trips_ts)


# This function is to get homogeneity score after the first/second round of clustering
# It is based on bin_trips, which are common trips. bin_trips are collected according to the indices of the trips
# in bins above cutoff
# More info about bin_trips is in similarity.py (delete_bins)
# The homogeneity score reflects the degree to which a cluster consists only of trips with similar ground truthed labels.
# In the following examples, "A","B","C" are user labels.
# The labels can be drawn from different sets as long as the mapping is unique (e.g. ["A", "A", "C"] matches perfectly
# with [0,0,1]). 
# Ideally, there would be 1:1 mapping between labels and clusters - e.g. ["A", "A", "A"] maps to [1,1,1]
# This can break in two ways:
# user label A maps to different clusters - e.g. ["A", "A", "A"] maps to [1,2,3].
# In this case, the homogeneity score will still be 1.0, since each cluster only has label "A".
# For our problem, this would typically map to the use case where trips with same user labels are actually to different 
# destinations. For `medical` or `personal` locations, for example, users could actually go to multiple medical 
# facilities or friends' houses. In this case, the trips will be in different clusters, but since the destinations are in 
# fact different, this would actually be the correct behavior.
# The trips could also be to the same location, but be clustered differently due to minor variations in duration or 
# distance (maybe due to traffic conditions). This could result in multiple clusters for what is essentially the same 
# trip. We capture this difference through the request percentage metric, which will result in three queries for 
# [1,2,3] and only one for [1,1,1]
# two different labels map to the same cluster - e.g. ["A", "A", "B"] maps to [1,1,1]. This is the case captured by the
# homogeneity score, which will be less than 1.0 (0 representes inhomogeneous, 1.0 represents homogeneous).
# This maps well to our use case because in this case, assigning the same label to all trips in the cluster would
# be incorrect. In particular, if we did not have the ground truth, the third trip would be labeled "A", 
# which would lower the accuracy.
# At this point, we didn't make user_input have same labels for labels_true and labels_pred.
# For example, in the second round, user labels are [("home", "ebike", "bus"),("home", "walk", "bus"),
# ("home", "ebike", "bus")], the labels_pred can be [0,1,0], or [1,0,1] or represented by other numeric labels.
def score(bin_trips, labels_pred):
    bin_trips_user_input_df = pd.DataFrame(data=[trip["data"]["user_input"] for trip in bin_trips])
    bin_trips_user_input_df = label_pro.map_labels(bin_trips_user_input_df)

    # turn all user_input into list without binning
    bin_trips_user_input_ls = bin_trips_user_input_df.values.tolist()
    # drop duplicate user_input
    no_dup_df = bin_trips_user_input_df.drop_duplicates()
    # turn non-duplicate user_input into list
    no_dup_list = no_dup_df.values.tolist()

    # collect labels_true based on user_input
    # To compute labels_true, we need to find out non-duplicate user labels, and use the index of the unique user label
    # to label the whole trips
    # If user labels are [(purpose, confirmed_mode, replaced_mode)]
    # e.g.,[("home","ebike","bus"),("work","walk","bike"),("home","ebike","bus"),("home","ebike","bus"),
    # ("work","walk","bike"),("exercise","ebike","walk")],
    # the unique label list is [0,1,2], labels_true will be [0,1,0,0,1,2]
    # labels_pred is the flattened list of labels of all common trips, e.g.[1,1,11,12,13,22,23]
    labels_true = []
    for userinput_dict in bin_trips_user_input_ls:
        if userinput_dict in no_dup_list:
            labels_true.append(no_dup_list.index(userinput_dict))

    labels_pred = labels_pred
    homo_score = skm.homogeneity_score(labels_true, labels_pred)
    return homo_score
