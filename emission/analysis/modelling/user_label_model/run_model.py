import logging
from typing import Optional

import arrow
import emission.analysis.modelling.similarity.od_similarity as eamso
import emission.analysis.modelling.user_label_model.greedy_similarity_binning as eamug
import emission.analysis.modelling.user_label_model.model_storage as eamums
import emission.analysis.modelling.user_label_model.model_type as eamumt
import emission.analysis.modelling.user_label_model.user_label_prediction_model as eamuu
import emission.core.wrapper.confirmedtrip as ecwc
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.pipeline_queries as epq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt

SIMILARITY_THRESHOLD_METERS = 500  # should come from app config


def update_user_label_model(
    user_id, 
    model_type: eamumt.ModelType, 
    model_storage: eamums.ModelStorage = eamums.ModelStorage.DATABASE, 
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
    model_data_prev = eamums.load_model(user_id, model_type, model_storage)
    if model_data_prev is not None:
        model.from_dict(model_data_prev)
        logging.debug(f"loaded {model_type.name} user label model for user {user_id}")
    else:
        logging.debug(f"building first {model_type.name} user label model for user {user_id}")

    # get all relevant trips
    time_query = epq.get_time_query_for_user_label_model(user_id) if model.is_incremental else None
    trips = _get_trips_for_user(user_id, time_query, min_trips)

    # train and store the model
    model.fit(trips)
    model_data_next = model.to_dict()
    eamums.save_model(user_id, model_type, model_data_next, timestamp, model_storage)

    logging.debug(f"{model_type.name} label prediction model built for user {user_id} with timestamp {timestamp}")


def predict_labels_with_n(
    trip: ecwc.Confirmedtrip,
    model_type = eamumt.ModelType.GREEDY_SIMILARITY_BINNING,
    model_storage = eamums.ModelStorage.DATABASE):
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


def model_factory(model_type: eamumt.ModelType) -> eamuu.UserLabelPredictionModel:
    """
    instantiates the requested user model type with the configured
    parameters. 
    
    hey YOU! if future model types are created, they should be added here!

    :param model_type: internally-used model name (an enum)
    :return: a user label prediction model
    :raises KeyError: if the requested model name does not exist
    """
    MODELS = {
        eamumt.ModelType.GREEDY_SIMILARITY_BINNING: eamug.GreedySimilarityBinning(
            metric=eamso.OriginDestinationSimilarity(),
            sim_thresh=SIMILARITY_THRESHOLD_METERS,
            apply_cutoff=False
        )
    }
    model = MODELS.get(model_type)
    if model is None:
        if not isinstance(model_type, eamumt.ModelType):
            raise TypeError(f"provided model type {model_type} is not an instance of ModelType")
        else:
            model_names = list(lambda e: e.name, MODELS.keys())
            models = ",".join(model_names)
            raise KeyError(f"user label model {model_type.name} not found in factory, must be one of {{{models}}}")
    return model


def _get_trips_for_user(user_id, time_query: Optional[estt.TimeQuery], min_trips: int):
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
    model_type: eamumt.ModelType, 
    model_storage: eamums.ModelStorage) -> Optional[eamuu.UserLabelPredictionModel]:
    """helper to build a user label prediction model class with the 
    contents of a stored model for some user.

    :param user_id: user to retrieve the model for
    :param model_type: UserLabelPredictionModel type configured for this OpenPATH server
    :param model_storage: storage type
    :return: model, or None if no model is stored for this user
    """
    model_dict = eamums.load_model(user_id, model_type, model_storage)
    if model_dict is None:
        return None
    else:    
        model = model_factory(model_type)
        model.from_dict(model_dict)
        return model
    
