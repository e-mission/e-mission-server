import emission.analysis.modelling.tour_model.similarity as similarity
import numpy as np
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.get_request_percentage as grp
import emission.analysis.modelling.tour_model.get_scores as gs
import emission.analysis.modelling.tour_model.label_processing as lp
import emission.analysis.modelling.tour_model.data_preprocessing as preprocess
import emission.analysis.modelling.tour_model.second_round_of_clustering as sr
import emission.analysis.modelling.tour_model.get_users as gu
import pandas as pd
import jsonpickle as jpickle

def second_round(bin_trips,filter_trips,first_labels,track,low,dist_pct,sim,kmeans):
    sec = sr.SecondRoundOfClustering(bin_trips,first_labels)
    first_label_set = list(set(first_labels))
    for l in first_label_set:
        sec.get_sel_features_and_trips(first_labels,l)
        sec.hierarcial_clustering(low, dist_pct)
        if kmeans:
            sec.kmeans_clustering()
        new_labels = sec.get_new_labels(first_labels)
        track = sec.get_new_track(track)
    # get request percentage for the subset for the second round
    percentage_second = grp.get_req_pct(new_labels, track, filter_trips, sim)
    # get homogeneity score for the second round
    homo_second = gs.score(bin_trips, new_labels)
    return percentage_second,homo_second


# we use functions in similarity to build the first round of clustering
def first_round(data,radius):
    sim = similarity.similarity(data, radius)
    filter_trips = sim.data
    sim.bin_data()
    sim.delete_bins()
    bins = sim.bins
    bin_trips = sim.newdata
    return sim, bins, bin_trips, filter_trips


def get_first_label(bins):
    # get first round labels
    # the labels from the first round are the indices of bins
    # e.g. in bin 0 [trip1, trip2, trip3], the labels of this bin is [0,0,0]
    first_labels = []
    for b in range(len(bins)):
        for trip in bins[b]:
            first_labels.append(b)
    return first_labels


def get_track(bins, first_labels):
    # create a list idx_labels_track to store indices and labels
    # the indices of the items will be the same in the new label list after the second round clustering
    # item[0] is the original index of the trip in filter_trips
    # item[1] is the label from the first round of clustering
    idx_labels_track = []
    for bin in bins:
        for ori_idx in bin:
            idx_labels_track.append([ori_idx])
    # store first round labels in idx_labels_track list
    for i in range(len(first_labels)):
        idx_labels_track[i].append(first_labels[i])

    return idx_labels_track


def get_first_label_and_track(bins,bin_trips,filter_trips):
    gs.compare_trip_orders(bins, bin_trips, filter_trips)
    first_labels = get_first_label(bins)
    track = get_track(bins, first_labels)
    return first_labels,track


def tune(data,radius,kmeans):
    sim, bins, bin_trips, filter_trips = first_round(data, radius)
    # it is possible that we don't have common trips for tuning or testing
    # bins contain common trips indices
    if len(bins) is not 0:
        first_labels, track = get_first_label_and_track(bins,bin_trips,filter_trips)
        # collect tuning scores and parameters
        tune_score = {}
        for dist_pct in np.arange(0.15, 0.6, 0.02):
            for low in range(250, 600):

                percentage_second, homo_second = second_round(bin_trips,filter_trips,first_labels,track,low,dist_pct,
                                                              sim,kmeans)

                curr_score = gs.get_score(homo_second, percentage_second)
                if curr_score not in tune_score:
                    tune_score[curr_score] = (low, dist_pct)

        best_score = max(tune_score)
        sel_tradeoffs = tune_score[best_score]
        low = sel_tradeoffs[0]
        dist_pct = sel_tradeoffs[1]
    else:
        low = 0
        dist_pct = 0

    return low,dist_pct


def test(data,radius,low,dist_pct,kmeans):
    sim, bins, bin_trips, filter_trips = first_round(data, radius)
    # it is possible that we don't have common trips for tuning or testing
    # bins contain common trips indices
    if len(bins) is not 0:
        first_labels, track = get_first_label_and_track(bins,bin_trips,filter_trips)
        # new_labels temporary stores the labels from the first round, but later the labels in new_labels will be
        # updated with the labels after two rounds of clustering.
        new_labels = first_labels.copy()
        # get request percentage for the subset for the first round
        percentage_first = grp.get_req_pct(new_labels, track, filter_trips, sim)
        # get homogeneity score for the subset for the first round
        homo_first = gs.score(bin_trips, first_labels)
        percentage_second, homo_second = second_round(bin_trips, filter_trips, first_labels, track, low, dist_pct,
                                                      sim, kmeans)
    else:
        percentage_first = 1
        homo_first = 1
        percentage_second = 1
        homo_second = 1
    scores = gs.get_score(homo_second, percentage_second)
    return homo_first,percentage_first,homo_second,percentage_second,scores


def main(all_users):
    radius = 100
    # get all/valid user list
    user_ls, valid_users = gu.get_user_ls(all_users, radius)
    all_filename = []
    for a in range(len(all_users)):
        user = all_users[a]
        df = pd.DataFrame(columns=['user','user_id','percentage of 1st round','homogeneity socre of 1st round',
                                   'percentage of 2nd round','homogeneity socre of 2nd roun','scores','lower boundary',
                                   'distance percentage'])
        trips = preprocess.read_data(user)
        filter_trips = preprocess.filter_data(trips, radius)
        # filter out users that don't have enough valid labeled trips
        if not gu.valid_user(filter_trips, trips):
            continue
        tune_idx, test_idx = preprocess.split_data(filter_trips)
        # choose tuning/test set to run the model
        # this step will use KFold (5 splits) to split the data into different subsets
        # - tune: tuning set
        # - test: test set
        # Here we user a bigger part of the data for testing and a smaller part for tuning
        tune_data = preprocess.get_subdata(filter_trips, test_idx)
        test_data = preprocess.get_subdata(filter_trips, tune_idx)

        # tune data
        for j in range(len(tune_data)):
            # for tuning, we don't add kmeans for re-clustering. We just need to get tuning parameters
            # - low: the lower boundary of the dendrogram. If the final distance of the dendrogram is lower than "low",
            # this bin no need to be re-clutered.
            # - dist_pct: the higher boundary of the dendrogram. If the final distance is higher than "low",
            # the cutoff of the dendrogram is (the final distance of the dendrogram * dist_pct)
            low, dist_pct = tune(tune_data[j], radius, kmeans=False)
            df.loc[j,'lower boundary']=low
            df.loc[j,'distance percentage']=dist_pct

        # testing
        for k in range(len(test_data)):
            low = df.loc[k,'lower boundary']
            dist_pct = df.loc[k,'distance percentage']

            # for testing, we add kmeans to re-build the model
            homo_first, percentage_first, homo_second, percentage_second, scores = test(test_data[k],radius,low,
                                                                                        dist_pct,kmeans=True)
            df.loc[k, 'percentage of 1st round'] = percentage_first
            df.loc[k, 'homogeneity socre of 1st round'] = homo_first
            df.loc[k, 'percentage of 2nd round'] = percentage_second
            df.loc[k, 'homogeneity socre of 2nd round'] = homo_second
            df.loc[k, 'scores'] = scores
            df['user_id'] = user
            df['user']='user'+str(a+1)

        filename = "user_" + str(user) + ".csv"
        all_filename.append(filename)
        df.to_csv(filename, index=True, index_label='split')

    # collect filename in a file, use it to plot the scatter
    collect_filename = jpickle.dumps(all_filename)
    with open("collect_filename", "w") as fd:
        fd.write(collect_filename)


if __name__ == '__main__':
    participant_uuid_obj = list(edb.get_profile_db().find({"install_group": "participant"}, {"user_id": 1, "_id": 0}))
    all_users = [u["user_id"] for u in participant_uuid_obj]
    main(all_users)
