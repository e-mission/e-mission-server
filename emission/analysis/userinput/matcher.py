import logging
import copy

# Get the configuration for the classifier
import emission.analysis.config as eac

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.decorations.trip_queries as esdt
import emission.storage.pipeline_queries as epq
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.tripuserinput as ecwtui

obj_to_dict_key = lambda key: key.split("/")[1]

def match_incoming_user_inputs(user_id):
    time_query = epq.get_time_range_for_incoming_userinput_match(user_id)
    try:
        last_user_input_done = match_incoming_inputs(user_id, time_query)
        if last_user_input_done is None:
            logging.debug("after run, last_user_input_done == None, must be early return")
            epq.mark_incoming_userinput_match_done(user_id, None)
        else:
            epq.mark_incoming_userinput_match_done(user_id, last_user_input_done.metadata.write_ts)
    except:
        logging.exception("Error while matching incoming user inputs, timestamp is unchanged")
        epq.mark_incoming_userinput_match_failed(user_id)

def match_incoming_inputs(user_id, timerange):
    ts = esta.TimeSeries.get_time_series(user_id)
    single_most_recent_match_key_list = eac.get_config()["userinput.keylist"]
    multi_non_deleted_key_list = ["manual/trip_addition_input"]
    input_key_list = single_most_recent_match_key_list + multi_non_deleted_key_list
    toMatchInputs = [ecwe.Entry(e) for e in ts.find_entries(input_key_list, time_query=timerange)]
    logging.debug("Matching %d single inputs to trips" % len(toMatchInputs))
    lastInputProcessed = None
    if len(toMatchInputs) == 0:
        logging.debug("len(toMatchInputs) == 0, early return")
        return None
    for ui in toMatchInputs:
        confirmed_trip = esdt.get_trip_for_user_input_obj(ts, ui)
        if confirmed_trip is not None:
            if ui.metadata.key in single_most_recent_match_key_list:
                handle_single_most_recent_match(confirmed_trip, ui)
            elif ui.metadata.key in multi_non_deleted_key_list:
                handle_multi_non_deleted_match(confirmed_trip, ui)
            else:
                assert False, "Found weird key {ui.metadata.key} that was not in the search list"
            import emission.storage.timeseries.builtin_timeseries as estbt
            estbt.BuiltinTimeSeries.update(confirmed_trip)
        else:
            logging.warn("No match found for single user input %s, moving forward anyway" % ui)
        lastInputProcessed = ui

    return lastInputProcessed

def handle_single_most_recent_match(confirmed_trip, ui):
    input_name = obj_to_dict_key(ui.metadata.key)
    if input_name == "trip_user_input":
        confirmed_trip["data"]["user_input"][input_name] = ui
    else:
        confirmed_trip["data"]["user_input"][input_name] = ui.data.label

def handle_multi_non_deleted_match(confirmed_object, ui):
    logging.debug(f"handling user input {ui} for {confirmed_object}")
    if "additions" not in confirmed_object["data"] or \
        confirmed_object["data"]["additions"] is None:
        confirmed_object["data"]["additions"] = []
    if "status" not in ui.data or ui.data.status == ecwtui.InputStatus.ACTIVE:
        confirmed_object["data"]["additions"].append(ui)
    elif ui.data.status == ecwtui.InputStatus.DELETED:
        after_del_list = [ta for ta in confirmed_object["data"]["additions"] if ta["match_id"] != ui["match_id"]]
        confirmed_object["data"]["additions"] = after_del_list
    else:
        # TODO: Decide whether to error or to warn here
        logging.warn("Invalid status found in user input %s, moving forward anyway" % ui)

def create_composite_objects(user_id):
    time_query = epq.get_time_range_for_composite_object_creation(user_id)
    ts = esta.TimeSeries.get_time_series(user_id)
    confirmedTrips = esda.get_entries(esda.CONFIRMED_TRIP_KEY, user_id, time_query=time_query)
    logging.info("Creating composite trips from %d confirmed trips" % len(confirmedTrips))
    if len(confirmedTrips) == 0:
        logging.debug("len(confirmedTrips) == 0, early return")
        return None
    for ct in confirmedTrips:
        logging.info("End place type for trip is %s" %  type(ct.data.end_place))
        end_place = esda.get_entry(esda.CONFIRMED_PLACE_KEY, ct["data"]["confirmed_place"])
        composite_trip_dict = copy.copy(ct)
        del composite_trip_dict["_id"]
        composite_trip_dict["metadata"]["key"] = "analysis/composite_trip"
        composite_trip_dict["data"]["confirmed_place"] = end_place

        # later we will want to put section & modes in composite_trip as well

        composite_trip_entry = ecwe.Entry(composite_trip_dict)
        # save the entry
        ts.insert(composite_trip_entry)
    epq.mark_composite_object_creation_done(user_id, None)

def create_confirmed_objects(user_id):
    time_query = epq.get_time_range_for_confirmed_object_creation(user_id)
    try:
        # we will use the same query for trips and places, but using 'exit_ts' instead 'end_ts'
        time_query.timeType = "data.exit_ts"
        last_expected_place_done = create_confirmed_places(user_id, time_query)
        time_query.timeType = "data.end_ts"
        last_expected_trip_done = create_confirmed_trips(user_id, time_query, last_expected_place_done["_id"])
        if last_expected_trip_done is None or last_expected_place_done is None:
            logging.debug("after run, last_expected_trip_done == None, must be early return")
            epq.mark_confirmed_object_creation_done(user_id, None)
        else:
            epq.mark_confirmed_object_creation_done(user_id, last_expected_trip_done.data.end_ts)

    except:
        logging.exception("Error while creating confirmed objects, timestamp is unchanged")
        epq.mark_confirmed_object_creation_failed(user_id)

def create_confirmed_places(user_id, timerange):
    ts = esta.TimeSeries.get_time_series(user_id)
    toConfirmPlaces = esda.get_entries(esda.CLEANED_PLACE_KEY, user_id,
        time_query=timerange)
    logging.info("Converting %d cleaned places to confirmed ones" % len(toConfirmPlaces))
    lastPlaceProcessed = None
    if len(toConfirmPlaces) == 0:
        logging.debug("len(toConfirmPlaces) == 0, early return")
        return None
    input_key_list = eac.get_config()["userinput.keylist"]
    for tcp in toConfirmPlaces:
        # Copy the place and fill in the new values
        confirmed_place_dict = copy.copy(tcp)
        del confirmed_place_dict["_id"]
        confirmed_place_dict["metadata"]["key"] = "analysis/confirmed_place"
        confirmed_trip_dict["data"]["cleaned_place"] = tcp.get_id()
        confirmed_place_dict["data"]["user_input"] = \
           get_user_input_dict(ts, tcp, input_key_list)
        confirmed_place_dict["data"]["additions"] = \
            esdt.get_additions_for_timeline_entry_object(ts, tcp)
        confirmed_place_entry = ecwe.Entry(confirmed_place_dict)
        # save the entry
        ts.insert(confirmed_place_entry)
        # if everything is successful, then update the last successful place
        lastPlaceProcessed = confirmed_place_entry

    return lastPlaceProcessed

def create_confirmed_trips(user_id, timerange, confirmed_place_id=None):
    ts = esta.TimeSeries.get_time_series(user_id)
    toConfirmTrips = esda.get_entries(esda.EXPECTED_TRIP_KEY, user_id,
        time_query=timerange)
    logging.debug("Converting %d expected trips to confirmed ones" % len(toConfirmTrips))
    lastTripProcessed = None
    if len(toConfirmTrips) == 0:
        logging.debug("len(toConfirmTrips) == 0, early return")
        return None
    input_key_list = eac.get_config()["userinput.keylist"]
    for tct in toConfirmTrips:
        # Copy the trip and fill in the new values
        confirmed_trip_dict = copy.copy(tct)
        del confirmed_trip_dict["_id"]
        confirmed_trip_dict["metadata"]["key"] = "analysis/confirmed_trip"
        confirmed_trip_dict["data"]["expected_trip"] = tct.get_id()
        confirmed_trip_dict["data"]["confirmed_place"] = confirmed_place_id
        confirmed_trip_dict["data"]["user_input"] = \
            get_user_input_dict(ts, tct, input_key_list)
        confirmed_trip_dict["data"]["additions"] = \
            esdt.get_additions_for_timeline_entry_object(ts, tct)
        confirmed_trip_entry = ecwe.Entry(confirmed_trip_dict)
        # save the entry
        ts.insert(confirmed_trip_entry)
        # if everything is successful, then update the last successful trip
        lastTripProcessed = confirmed_trip_entry

    return lastTripProcessed

def get_user_input_dict(ts, tct, input_key_list):
    tct_userinput = {}
    for ikey in input_key_list:
        matched_userinput = esdt.get_user_input_for_timeline_entry_object(ts, tct, ikey)
        if matched_userinput is not None:
            ikey_name = obj_to_dict_key(ikey)
            if ikey_name == "trip_user_input":
                tct_userinput[ikey_name] = matched_userinput
            else:
                tct_userinput[ikey_name] = matched_userinput.data.label
    logging.debug("for trip %s, returning user input dict %s" % (tct.get_id(), tct_userinput))
    return tct_userinput
