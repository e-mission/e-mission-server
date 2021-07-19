import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.get_scores as gs
import emission.analysis.modelling.tour_model.get_users as gu
import emission.analysis.modelling.tour_model.label_processing as lp
import evaluation_pipeline as ep
import load_predict as load
import emission.analysis.modelling.tour_model.data_preprocessing as preprocess
import copy
import pandas as pd
import logging
import jsonpickle as jpickle
import sklearn.cluster as sc


def find_best_split_and_parameters(user,test_data):
    # find the best score
    filename = "user_"+str(user)+".csv"
    df = pd.read_csv(filename, index_col='split')
    scores = df['scores'].tolist()
    best_split_idx = scores.index(max(scores))
    # use the position of best_score to find best_split
    best_split = test_data[best_split_idx]
    # use best_split_idx to find the best parameters
    low = df.loc[best_split_idx, 'lower boundary']
    dist_pct = df.loc[best_split_idx, 'distance percentage']
    return best_split,best_split_idx,low,dist_pct


# def find_best_parameters(user,best_split_idx):
#     tradeoff_filename = 'tradeoff_' + str(user)
#     tradeoff_1user = load.loadModelStage(tradeoff_filename)
#     best_parameters = tradeoff_1user[best_split_idx]
#     return best_parameters


def save_models(obj_name,obj,user):
    obj_capsule = jpickle.dumps(obj)
    filename = obj_name + '_' + str(user)
    with open(filename, "w") as fd:
        fd.write(obj_capsule)


def main():
    participant_uuid_obj = list(edb.get_profile_db().find({"install_group": "participant"}, {"user_id": 1, "_id": 0}))
    all_users = [u["user_id"] for u in participant_uuid_obj]
    radius = 100
    for a in range(len(all_users)):
        user = all_users[a]
        trips = preprocess.read_data(user)
        filter_trips = preprocess.filter_data(trips, radius)
        # filter out users that don't have enough valid labeled trips
        if not gu.valid_user(filter_trips, trips):
            logging.debug("This user doesn't have enough valid trips for further analysis.")
            continue
        tune_idx, test_idx = preprocess.split_data(filter_trips)
        test_data = preprocess.get_subdata(filter_trips, tune_idx)

        # find the best split and parameters, and use them to build the model
        best_split, best_split_idx, low, dist_pct = find_best_split_and_parameters(user,test_data)

        # run the first round of clustering
        sim, bins, bin_trips, filter_trips = ep.first_round(best_split, radius)
        # It is possible that the user doesn't have common trips. Here we only build models for user that has common trips.
        if len(bins) is not 0:
            gs.compare_trip_orders(bins, bin_trips, filter_trips)
            first_labels = ep.get_first_label(bins)
            first_label_set = list(set(first_labels))

            # second round of clustering
            model_coll = {}
            bin_loc_feat = {}
            fitst_round_labels = {}
            for fl in first_label_set:
                # store second round trips data
                second_round_trips = []
                for index, first_label in enumerate(first_labels):
                    if first_label == fl:
                        second_round_trips.append(bin_trips[index])
                x = preprocess.extract_features(second_round_trips)
                # collect location features of the bin from the first round of clustering
                # feat[0:4] are start/end coordinates
                bin_loc_feat[str(fl)] = [feat[0:4] for feat in x]
                # here we pass in features(x) from selected second round trips to build the model
                method = 'single'
                clusters = lp.get_second_labels(x, method, low, dist_pct)
                n_clusters = len(set(clusters))
                # build the model
                kmeans = sc.KMeans(n_clusters=n_clusters, random_state=0).fit(x)
                # collect all models, the key is the label from the 1st found
                # e.g.{'0': KMeans(n_clusters=2, random_state=0)}
                model_coll[str(fl)] = kmeans
                # get labels from the 2nd round of clustering
                second_labels = kmeans.labels_

                first_label_obj = []

                # save user labels for every cluster
                second_label_set = list(set(second_labels))
                sec_round_labels = {}
                for sl in second_label_set:
                    sec_sel_trips = []
                    sec_label_obj = []
                    for idx, second_label in enumerate(second_labels):
                        if second_label == sl:
                            sec_sel_trips.append(second_round_trips[idx])
                    user_label_df = pd.DataFrame([trip['data']['user_input'] for trip in sec_sel_trips])
                    user_label_df = lp.map_labels(user_label_df)
                    # compute the sum of trips in this cluster
                    sum_trips = len(user_label_df)
                    # compute unique label sets and their probabilities in one cluster
                    # 'p' refers to probability
                    unique_labels = user_label_df.groupby(user_label_df.columns.tolist()).size().reset_index(name='count')
                    unique_labels['p'] = unique_labels.count / sum_trips
                    labels_columns = user_label_df.columns.to_list()
                    for i in range(len(unique_labels)):
                        one_set_labels = {}
                        # e.g. labels_only={'mode_confirm': 'pilot_ebike', 'purpose_confirm': 'work', 'replaced_mode': 'walk'}
                        labels_only = {column: unique_labels.iloc[i][column] for column in labels_columns}
                        one_set_labels["labels"] = labels_only
                        one_set_labels['p'] = unique_labels.iloc[i]['p']
                        # e.g. one_set_labels = {'labels': {'mode_confirm': 'walk', 'replaced_mode': 'walk', 'purpose_confirm': 'exercise'}, 'p': 1.0}
                        # in case append() method changes the dict, we use deepcopy here
                        labels_set = copy.deepcopy(one_set_labels)
                        sec_label_obj.append(labels_set)

                    # put user labels from the 2nd round into a dict, the key is the label from the 2nd round of clustering
                    #e.g. {'0': [{'labels': {'mode_confirm': 'bus', 'replaced_mode': 'bus', 'purpose_confirm': 'home'}, 'p': 1.0}]}
                    sec_round_labels[str(sl)] = sec_label_obj
                sec_round_collect = copy.deepcopy(sec_round_labels)
                # collect all user labels from the 2nd round, the key is to the label from the 1st round
                # e.g. fitst_round_labels = {'0': [{'0': [{'labels': {'mode_confirm': 'drove_alone', 'purpose_confirm': 'work', 'replaced_mode': 'drove_alone'}, 'p': 1.0}]}]}
                first_label_obj.append(sec_round_collect)
                fitst_round_labels[str(fl)] = first_label_obj
            # wrap up all labels
            # e.g. all_labels = [{'first_label': [{'second_label': [{'labels': {'mode_confirm': 'shared_ride',
            # 'purpose_confirm': 'home', 'replaced_mode': 'drove_alone'}, 'p': 1.0}]}]}]
            all_labels = [fitst_round_labels]

            # save all user labels
            save_models('user_labels',all_labels,user)

            # save models from the 2nd round of clustering
            save_models('models',[model_coll],user)

            # save location features of all bins
            save_models('locations',[bin_loc_feat],user)


if __name__ == '__main__':
    main()

















