import logging
from typing import List, Optional
from uuid import UUID
import time
import arrow 

import emission.storage.timeseries.timequery as estt
import emission.analysis.modelling.trip_model.model_storage as eamums
import emission.analysis.modelling.trip_model.model_type as eamumt
import emission.analysis.modelling.trip_model.trip_model as eamuu
import emission.core.wrapper.confirmedtrip as ecwc
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.pipeline_queries as epq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt


def update_trip_model(
    user_id, 
    model_type: eamumt.ModelType, 
    model_storage: eamums.ModelStorage = eamums.ModelStorage.DOCUMENT_DATABASE, 
    min_trips: int = 14,
    model_config = None
    ):
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
    :param model_config: optional configuration for model, for debugging purposes
    """
    try:
        # this timestamp is used for recording the state of the updated model
        timestamp = time.time()
        model = model_type.build(model_config)

        # if a previous model exists, deserialize the stored model
        model_data_prev = eamums.load_model(user_id, model_type, model_storage)
        stored_model_exists = model_data_prev is not None
        if stored_model_exists:
            model.from_dict(model_data_prev)
            logging.debug(f"loaded {model_type.name} user label model for user {user_id}")
        else:
            logging.debug(f"building first {model_type.name} user label model for user {user_id}")

        # must call this regardless of whether model is incremental or not as it has
        # the additional effect of marking the start state of the pipeline execution
        time_query_from_pipeline = epq.get_time_query_for_trip_model(user_id)
        time_query = time_query_from_pipeline if model.is_incremental else None
        logging.debug(f'model type {model_type.name} is incremental? {model.is_incremental}')
        logging.debug(f'time query for training data collection: {time_query}')

        trips = _get_training_data(user_id, time_query)
        
        # don't start training for a user that doesn't have at least $trips many trips
        # (assume if a stored model exists for the user, that they met this requirement previously)
        if len(trips) == 0:
            msg = f"no new confirmed trips found in database to train model for user {user_id}"
            logging.debug(msg)
            epq.mark_trip_model_failed(user_id)
        elif not stored_model_exists and not len(trips) >= min_trips:
            msg = (
                f"Total: {len(trips)}, labeled: {len(trips)}, user "
                f"{user_id} doesn't have enough valid trips for further analysis."
            )
            logging.debug(msg)
            epq.mark_trip_model_failed(user_id)
        else:
            
            # train and store the model
            model.fit(trips)
            model_data_next = model.to_dict()

            if len(model_data_next) == 0:
                epq.mark_trip_model_failed(user_id)
                msg = f"trip model for user {user_id} is empty"
                raise Exception(msg)
            
            last_done_ts = _latest_timestamp(trips)
            eamums.save_model(user_id, model_type, model_data_next, last_done_ts, model_storage)
            logging.debug(f"{model_type.name} label prediction model built for user {user_id} with timestamp {last_done_ts}")

            epq.mark_trip_model_done(user_id, last_done_ts)
        
    except Exception as e:
        epq.mark_trip_model_failed(user_id)
        msg = (
            f"failure updating user label pipeline state for user {user_id}"
        )
        raise IOError(msg) from e


def predict_labels_with_n(
    trip_list: List[ecwc.Confirmedtrip],
    model: eamuu.TripModel):
    """
    invoke the user label prediction model to predict labels for a trip.

    :param trip_list: the list of trips to predict labels for
    :param model: trip model used for predictions
    :return: a list of predictions
    """

    predictions_list = []
    print(f"{arrow.now()} Inside predict_labels_n: Predicting...")
    start_predict_time = time.process_time()
    for trip in trip_list:
        if model is None:
            predictions_list.append(([], -1))
            continue
        else:
            predictions, n = model.predict(trip)
            predictions_list.append((predictions, n))
    print(f"{arrow.now()} Inside predict_labels_n: Predictions complete for trip_list in time = {time.process_time() - start_predict_time}")
    return predictions_list


def _get_training_data(user_id: UUID, time_query: Optional[estt.TimeQuery]):
    """
    load the labeled trip data for this user, subject to a time query. if the user
    does not have at least $min_trips trips with labels, then return an empty list.

    :param user_id: user to collect trips from
    :param time_query: query to restrict the time (optional)
    """
    
    ts = esta.TimeSeries.get_time_series(user_id)
    trips = list(ts.find_entries([esda.CONFIRMED_TRIP_KEY], time_query=time_query))  
    print(f'found {len(trips)} training rows')
    labeled_trips = [trip for trip in trips if trip['data']['user_input'] != {}]

    logging.debug(f'found {len(labeled_trips)} labeled trips for user {user_id}')
    return labeled_trips


def _load_stored_trip_model(
    user_id, 
    model_type: eamumt.ModelType, 
    model_storage: eamums.ModelStorage,
    model_config = None) -> Optional[eamuu.TripModel]:
    """helper to build a user label prediction model class with the 
    contents of a stored model for some user.

    :param user_id: user to retrieve the model for
    :param model_type: TripModel type configured for this OpenPATH server
    :param model_storage: storage type
    :param model_config: optional configuration for model, for debugging purposes
    :return: model, or None if no model is stored for this user
    """
    model_dict = eamums.load_model(user_id, model_type, model_storage)
    if model_dict is None:
        return None
    else:    
        model = model_type.build(model_config)
        model.from_dict(model_dict)
        return model
    

def _latest_timestamp(trips: List[ecwc.Confirmedtrip]) -> float:
    """extract the latest timestamp observed from a list of trips

    :param trips: the trips to review
    :return: the latest timestamp
    """
    ts = sorted(trips, key=lambda r: r['data']['end_ts'], reverse=True)[0]['data']['end_ts']
    return ts