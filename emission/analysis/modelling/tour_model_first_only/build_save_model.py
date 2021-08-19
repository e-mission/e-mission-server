# Standard imports
import copy
import pandas as pd
import logging
import jsonpickle as jpickle
import sklearn.cluster as sc

# Our imports
import emission.storage.timeseries.abstract_timeseries as esta

import emission.analysis.modelling.tour_model_first_only.get_users as gu
import emission.analysis.modelling.tour_model.label_processing as lp
import emission.analysis.modelling.tour_model_first_only.evaluation_pipeline as ep
import emission.analysis.modelling.tour_model.load_predict as load
import emission.analysis.modelling.tour_model_first_only.data_preprocessing as preprocess

RADIUS=500

def save_models(obj_name,obj,user):
    obj_capsule = jpickle.dumps(obj)
    filename = obj_name + '_first_round_' + str(user)
    with open(filename, "w") as fd:
        fd.write(obj_capsule)

def create_location_map(trip_list, bins):
    bin_loc_feat = {}
    user_input_map = {}
    for i, curr_bin in enumerate(bins):
        # print(f"Considering {curr_bin} for trip list of length {len(trip_list)}")
        bin_trips = [trip_list[j] for j in curr_bin]
        # print(f"Considering {bin_trips} for bin {curr_bin}")
        x = preprocess.extract_features(bin_trips)
        bin_loc_feat[str(i)] = [feat[0:4] for feat in x]
    return bin_loc_feat

def create_user_input_map(trip_list, bins):
    # map from bin index to user input probabilities
    # e.g. {"0": [{'labels': {'mode_confirm': 'drove_alone', 'purpose_confirm': 'work', 'replaced_mode': 'drove_alone'}, 'p': 1.0}]}
    user_input_map = {}
    for b, curr_bin in enumerate(bins):
        bin_trips = [trip_list[j] for j in curr_bin]
        user_label_df = pd.DataFrame([trip['data']['user_input'] for trip in bin_trips])
        user_label_df = lp.map_labels(user_label_df).dropna()
        # compute the sum of trips in this cluster
        sum_trips = len(user_label_df)
        # compute unique label sets and their probabilities in one cluster
        # 'p' refers to probability
        unique_labels = user_label_df.groupby(user_label_df.columns.tolist()).size().reset_index(name='uniqcount')
        unique_labels['p'] = unique_labels.uniqcount / sum_trips
        labels_columns = user_label_df.columns.to_list()
        bin_label_combo_list = []
        for i in range(len(unique_labels)):
            one_set_labels = {}
            # e.g. labels_only={'mode_confirm': 'pilot_ebike', 'purpose_confirm': 'work', 'replaced_mode': 'walk'}
            labels_only = {column: unique_labels.iloc[i][column] for column in labels_columns}
            one_set_labels["labels"] = labels_only
            one_set_labels['p'] = unique_labels.iloc[i]['p']
            # e.g. one_set_labels = {'labels': {'mode_confirm': 'walk', 'replaced_mode': 'walk', 'purpose_confirm': 'exercise'}, 'p': 1.0}
            bin_label_combo_list.append(one_set_labels)
        user_input_map[str(b)] = bin_label_combo_list
    return user_input_map

def build_user_model(user):
    trips = preprocess.read_data(user)
    filter_trips = preprocess.filter_data(trips, RADIUS)
    # filter out users that don't have enough valid labeled trips
    if not gu.valid_user(filter_trips, trips):
        logging.debug(f"Total: {len(trips)}, labeled: {len(filter_trips)}, user {user} doesn't have enough valid trips for further analysis.")
        return
    # run the first round of clustering
    sim, bins, bin_trips, filter_trips = ep.first_round(filter_trips, RADIUS)

    # save all user labels
    save_models('user_labels',create_user_input_map(filter_trips, bins),user)

    # save location features of all bins
    save_models('locations',create_location_map(filter_trips, bins),user)

