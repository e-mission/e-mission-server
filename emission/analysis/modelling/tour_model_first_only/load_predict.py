# Standard imports
import jsonpickle as jpickle
import logging

# Our imports
import emission.storage.timeseries.abstract_timeseries as esta
import emission.analysis.modelling.tour_model.similarity as similarity
import emission.analysis.modelling.tour_model.similarity as similarity
import emission.analysis.modelling.tour_model.data_preprocessing as preprocess


def loadModelStage(filename):
    import jsonpickle.ext.numpy as jsonpickle_numpy
    jsonpickle_numpy.register_handlers()
    model = loadModel(filename)
    return model


def loadModel(filename):
    fd = open(filename, "r")
    all_model = fd.read()
    all_model = jpickle.loads(all_model)
    fd.close()
    return all_model


def in_bin(bin_location_features,new_trip_location_feat,radius):
    start_b_lon = new_trip_location_feat[0]
    start_b_lat = new_trip_location_feat[1]
    end_b_lon = new_trip_location_feat[2]
    end_b_lat = new_trip_location_feat[3]
    for feat in bin_location_features:
        start_a_lon = feat[0]
        start_a_lat = feat[1]
        end_a_lon = feat[2]
        end_a_lat = feat[3]
        start = similarity.within_radius(start_a_lat, start_a_lon, start_b_lat, start_b_lon,radius)
        end = similarity.within_radius(end_a_lat, end_a_lon, end_b_lat, end_b_lon, radius)
        if start and end:
            continue
        else:
            return False
    return True

def find_bin(trip, bin_locations, radius):
    trip_feat = preprocess.extract_features([trip])[0]
    trip_loc_feat = trip_feat[0:4]
    first_round_label_set = list(bin_locations.keys())
    sel_fl = None
    for fl in first_round_label_set:
        # extract location features of selected bin
        sel_loc_feat = bin_locations[fl]
        # Check if start/end locations of the new trip and every start/end locations in this bin are within the range of
        # radius. If so, the new trip falls in this bin. Then predict the second round label of the new trip
        # using this bin's model
        if in_bin(sel_loc_feat, trip_loc_feat, radius):
            sel_fl = fl
            break
    if not sel_fl:
        logging.debug(f"sel_fl = {sel_fl}, early return")
        return -1
    return sel_fl

def predict_labels(trip):
    radius = 100
    user = trip['user_id']
    logging.debug(f"At stage: extracting features")
    trip_feat = preprocess.extract_features([trip])[0]
    trip_loc_feat = trip_feat[0:4]
    logging.debug(f"At stage: loading model")
    try:
        # load locations of bins(1st round of clustering)
        # e.g.{'0': [[start lon1, start lat1, end lon1, end lat1],[start lon, start lat, end lon, end lat]]}
        # another explanation: -'0': label from the 1st round
        #                      - the value of key '0': all trips that in this bin
        #                      - for every trip: the coordinates of start/end locations
        bin_locations = loadModelStage('locations_first_round' + str(user))[0]

        # load user labels in all clusters
        # assume that we have 1 cluster(bin) from the 1st round of clustering, which has label '0',
        # and we have 1 cluster from the 2nd round, which has label '1'
        # the value of key '0' contains all 2nd round clusters
        # the value of key '1' contains all user labels and probabilities in this cluster
        # e.g. {'0': [{'1': [{'labels': {'mode_confirm': 'shared_ride', 'purpose_confirm': 'home', 'replaced_mode': 'drove_alone'}}]}]}
        user_labels = loadModelStage('user_labels_first_round' + str(user))[0]

    except IOError as e:
        logging.info(f"No models found for {user}, no prediction")
        logging.exception(e)
        return []


    logging.debug(f"At stage: first round prediction")
    pred_bin = find_bin(trip, bin_location, radius)
    logging.debug(f"At stage: matched with bin {pred_bin}")

    if pred_bin == -1:
        logging.info(f"No match found for {trip['data']['start_fmt_time']} early return")
        return []

    user_input_pred_list = user_labels[pred_bin]
    logging.debug(f"At stage: looked up user input {user_input_pred_list}")
    return user_input_pred_list

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
        level=logging.DEBUG)
    all_users = esta.TimeSeries.get_uuid_list()

    # case 1: the new trip matches a bin from the 1st round and a cluster from the 2nd round
    user = all_users[0]
    radius = 100
    trips = preprocess.read_data(user)
    filter_trips = preprocess.filter_data(trips, radius)
    new_trip = filter_trips[4]
    # result is [{'labels': {'mode_confirm': 'shared_ride', 'purpose_confirm': 'church', 'replaced_mode': 'drove_alone'},
    # 'p': 0.9333333333333333}, {'labels': {'mode_confirm': 'shared_ride', 'purpose_confirm': 'entertainment',
    # 'replaced_mode': 'drove_alone'}, 'p': 0.06666666666666667}]
    pl = predict_labels(new_trip)
    assert len(pl) > 0, f"Invalid prediction {pl}"

    # case 2: no existing files for the user who has the new trip:
    # 1. the user is invalid(< 10 existing fully labeled trips, or < 50% of trips that fully labeled)
    # 2. the user doesn't have common trips
    user = all_users[1]
    trips = preprocess.read_data(user)
    new_trip = trips[0]
    # result is []
    pl = predict_labels(new_trip)
    assert len(pl) == 0, f"Invalid prediction {pl}"

    # case3: the new trip is novel trip(doesn't fall in any 1st round bins)
    user = all_users[0]
    radius = 100
    trips = preprocess.read_data(user)
    filter_trips = preprocess.filter_data(trips, radius)
    new_trip = filter_trips[0]
    # result is []
    pl = predict_labels(new_trip)
    assert len(pl) == 0, f"Invalid prediction {pl}"

    # case 4: the new trip falls in a 1st round bin, but predict to be a new cluster in the 2nd round
    # result is []
    # no example for now
