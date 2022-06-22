import logging
from typing import Optional

import arrow
import emission.analysis.modelling.tour_model.cluster_pipeline as pipeline
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.pipeline_queries as epq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.analysis.modelling.similarity.od_similarity as eamso
import emission.analysis.modelling.user_label_model.greedy_similarity_binning as eamug
import emission.analysis.modelling.user_label_model.model_storage as eamum
from emission.analysis.modelling.user_label_model.model_storage import ModelStorage
from emission.analysis.modelling.user_label_model.model_type import ModelType
import emission.analysis.modelling.user_label_model.user_label_prediction_model as eamuu
from emission.core.wrapper.confirmedtrip import Confirmedtrip
from emission.storage.timeseries.timequery import TimeQuery
from numpy import isin

SIMILARITY_THRESHOLD_METERS = 500


def update_user_label_model(
    user_id, 
    model_type: ModelType, 
    model_storage: ModelStorage = ModelStorage.DATABASE, 
    min_trips: int = 14):
    """
    create/update a user label model for a user.

    updating the user label model occurs as a background task for the server. 
    trips for the user are collected and the data is fit to the requested model type.
    if the model type is "incremental", only the newest trips are used.

    :param user_id: id of user
    :param model_type: type of model to build. this is also stored on the database. if
                    there is a mismatch, an exception is thrown
    :param model_storage: storage destination for built model (default DATABASE)
    :param min_trips: minimum number of labeled trips per user to apply prediction (default 14)
    """

    # this timestamp is used for recording the state of the updated model
    timestamp = arrow.now()
    model = model_factory(model_type)

    # if a previous model exists, deserialize the stored model
    model_data_prev = eamum.load_model(user_id, model_type, model_storage)
    if model_data_prev is not None:
        model.from_dict(model_data_prev)

    # get all relevant trips
    time_query = epq.get_time_query_for_user_label_model(user_id) if model.is_incremental else None
    trips = _get_trips_for_user(user_id, time_query, min_trips)

    # train and store the model
    model.fit(trips)
    model_data_next = model.to_dict()
    eamum.save_model(user_id, model_type, model_data_next, timestamp, model_storage)

    logging.debug(f"{model_type.name} label prediction model built for user {user_id} with timestamp {timestamp}")


def predict_labels_with_n(
    trip: Confirmedtrip,
    model_type = ModelType.GREEDY_SIMILARITY_BINNING,
    model_storage = ModelStorage.DATABASE):
    """
    invoke the user label prediction model to predict labels for a trip.

    :param trip: the trip to predict labels for
    :param model_type: type of prediction model to run
    :param model_storage: location to read/write models
    :return: a list of predictions
    """
    user_id = trip['user_id']
    model = _load_user_label_model(user_id, model_type, model_storage)
    if model is None:
        return [], -1
    else:
        predictions, n = model.predict(trip)
        return predictions, n


def model_factory(model_type: ModelType) -> eamuu.UserLabelPredictionModel:
    """
    instantiates the requested user model type with the configured
    parameters. 
    
    hey YOU! if future model types are created, they should be added here!

    :param model_type: internally-used model name (an enum)
    :return: a user label prediction model
    :raises KeyError: if the requested model name does not exist
    """
    MODELS = {
        ModelType.GREEDY_SIMILARITY_BINNING: eamug.GreedySimilarityBinning(
            metric=eamso.OriginDestinationSimilarity(),
            sim_thresh=SIMILARITY_THRESHOLD_METERS,
            apply_cutoff=False
        )
    }
    model = MODELS.get(model_type)
    if model is None:
        if not isinstance(model_type, ModelType):
            raise TypeError(f"provided model type {model_type} is not an instance of ModelType")
        else:
            model_names = list(lambda e: e.name, MODELS.keys())
            models = ",".join(model_names)
            raise KeyError(f"user label model {model_type.name} not found in factory, must be one of {{{models}}}")
    return model


def _get_trips_for_user(user_id, time_query: Optional[TimeQuery], min_trips: int):
    """
    load the labeled trip data for this user, subject to a time query. if the user
    does not have at least $min_trips trips with labels, then return an empty list.

    :param user_id: user to collect trips from
    :param time_query: query to restrict the time (optional)
    :param min_trips: minimum number of labeled trips required to train
    """
    trips = esda.get_entries(key=esda.CONFIRMED_TRIP_KEY, user_id=user_id, time_query=time_query)
    labeled_trips = [trip for trip in trips if trip['data']['user_input'] != {}]
    if not len(labeled_trips) >= min_trips:
        msg = (
            f"Total: {len(trips)}, labeled: {len(labeled_trips)}, user "
            f"{user_id} doesn't have enough valid trips for further analysis."
        )
        logging.debug(msg)
        return []
    return labeled_trips


def _load_user_label_model(
    user_id, 
    model_type: ModelType, 
    model_storage: ModelStorage) -> Optional[eamuu.UserLabelPredictionModel]:
    """helper to build a user label prediction model class with the 
    contents of a stored model for some user.

    :param user_id: user to retrieve the model for
    :param model_type: UserLabelPredictionModel type configured for this OpenPATH server
    :param model_storage: storage type
    :return: model, or None if no model is stored for this user
    """
    model_dict = eamum.load_model(user_id, model_type, model_storage)
    if model_dict is None:
        return None
    else:    
        model = model_factory(model_type)
        model.from_dict(model_dict)
        return model
    

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',level=logging.DEBUG)
    all_users = esta.TimeSeries.get_uuid_list()

    # case 1: the new trip matches a bin from the 1st round and a cluster from the 2nd round
    user_id = all_users[0]
    update_user_label_model(user_id, ModelType.GREEDY_SIMILARITY_BINNING, ModelStorage.FILE_SYSTEM)
    filter_trips = _get_trips_for_user(user_id, None, 0)
    new_trip = filter_trips[4]
    # result is [{'labels': {'mode_confirm': 'shared_ride', 'purpose_confirm': 'church', 'replaced_mode': 'drove_alone'},
    # 'p': 0.9333333333333333}, {'labels': {'mode_confirm': 'shared_ride', 'purpose_confirm': 'entertainment',
    # 'replaced_mode': 'drove_alone'}, 'p': 0.06666666666666667}]
    pl, _ = predict_labels_with_n(new_trip)
    assert len(pl) > 0, f"Invalid prediction {pl}"

    # case 2: no existing files for the user who has the new trip:
    # 1. the user is invalid(< 10 existing fully labeled trips, or < 50% of trips that fully labeled)
    # 2. the user doesn't have common trips
    user_id = all_users[1]
    update_user_label_model(user_id, ModelType.GREEDY_SIMILARITY_BINNING, ModelStorage.FILE_SYSTEM)
    filter_trips = _get_trips_for_user(user_id, None, 0)
    new_trip = filter_trips[0]
    # result is []
    pl, _ = predict_labels_with_n(new_trip)
    assert len(pl) == 0, f"Invalid prediction {pl}"

    # case3: the new trip is novel trip(doesn't fall in any 1st round bins)
    user = all_users[0]
    update_user_label_model(user_id, ModelType.GREEDY_SIMILARITY_BINNING, ModelStorage.FILE_SYSTEM)
    filter_trips = _get_trips_for_user(user_id, None, 0)
    new_trip = filter_trips[0]
    # result is []
    pl = predict_labels_with_n(new_trip)
    assert len(pl) == 0, f"Invalid prediction {pl}"

    # case 4: the new trip falls in a 1st round bin, but predict to be a new cluster in the 2nd round
    # result is []
    # no example for now
