import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.analysis.modelling.tour_model.cluster_pipeline as pipeline
import emission.analysis.modelling.tour_model.similarity as similarity
import pandas as pd
from sklearn.model_selection import KFold


# read data that have user labels
def read_data(user):
    trips = pipeline.read_data(uuid=user, key=esda.CONFIRMED_TRIP_KEY)
    return trips


# - trips: all trips read from database
# - filter_trips: valid trips that have user labels and are not points
def filter_data(trips,radius):
    non_empty_trips = [t for t in trips if t["data"]["user_input"] != {}]
    non_empty_trips_df = pd.DataFrame(t["data"]["user_input"] for t in non_empty_trips)
    valid_trips_df = non_empty_trips_df.dropna(axis=0, how='any', thresh=None, subset=None, inplace=False)
    valid_trips_idx_ls = valid_trips_df.index.tolist()
    valid_trips = [non_empty_trips[i]for i in valid_trips_idx_ls]

    # similarity codes can filter out trips that are points in valid_trips
    filter_trips = similarity.filter_too_short(valid_trips, radius)
    return filter_trips


# use KFold (n_splits=5) to split the data into 5 models (5 training sets, 5 test sets)
def extract_features(filter_trips):
    X = []
    for trip in filter_trips:
        start = trip.data.start_loc["coordinates"]
        end = trip.data.end_loc["coordinates"]
        distance = trip.data.distance
        duration = trip.data.duration
        X.append([start[0], start[1], end[0], end[1], distance, duration])
    return X

def split_data(filter_trips):
    X = extract_features(filter_trips)
    kf = KFold(n_splits=5, shuffle=True, random_state=3)
    train_idx = []
    test_idx = []
    for train_index, test_index in kf.split(X):
        train_idx.append(train_index)
        test_idx.append(test_index)
    return train_idx, test_idx


# collect a set of data(training/test set) after splitting
def get_subdata(filter_trips,train_test_set):
    collect_sub_data = []
    for train_test_subset in train_test_set:
        sub_data = []
        for idx in train_test_subset:
            sub_data.append(filter_trips[idx])
        collect_sub_data.append(sub_data)
    return collect_sub_data
