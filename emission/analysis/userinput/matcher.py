import logging
import copy

# Get the configuration for the classifier
import emission.analysis.config as eac

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.decorations.trip_queries as esdt
import emission.storage.pipeline_queries as epq
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.userinput as ecwui

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
    multi_non_deleted_key_list = ["manual/trip_addition_input", "manual/place_addition_input"]
    input_key_list = single_most_recent_match_key_list + multi_non_deleted_key_list
    toMatchInputs = [ecwe.Entry(e) for e in ts.find_entries(input_key_list, time_query=timerange)]
    logging.debug("Matching %d single inputs to trips" % len(toMatchInputs))
    lastInputProcessed = None
    if len(toMatchInputs) == 0:
        logging.debug("len(toMatchInputs) == 0, early return")
        return None
    for ui in toMatchInputs:
        confirmed_obj = esdt.get_confirmed_obj_for_user_input_obj(ts, ui)
        if confirmed_obj is not None:
            if ui.metadata.key in single_most_recent_match_key_list:
                handle_single_most_recent_match(confirmed_obj, ui)
            elif ui.metadata.key in multi_non_deleted_key_list:
                handle_multi_non_deleted_match(confirmed_obj, ui)
            else:
                assert False, "Found weird key {ui.metadata.key} that was not in the search list"
            import emission.storage.timeseries.builtin_timeseries as estbt
            estbt.BuiltinTimeSeries.update(confirmed_obj)
        else:
            logging.warn("No match found for single user input %s, moving forward anyway" % ui)
        lastInputProcessed = ui

    return lastInputProcessed

def handle_single_most_recent_match(confirmed_obj, ui):
    input_name = obj_to_dict_key(ui.metadata.key)
    if input_name == "trip_user_input" or input_name == "place_user_input":
        confirmed_obj["data"]["user_input"][input_name] = ui
    else:
        confirmed_obj["data"]["user_input"][input_name] = ui.data.label

def handle_multi_non_deleted_match(confirmed_obj, ui):
    logging.debug(f"handling user input {ui} for {confirmed_obj}")
    if "additions" not in confirmed_obj["data"] or \
        confirmed_obj["data"]["additions"] is None:
        confirmed_obj["data"]["additions"] = []
    if "status" not in ui.data or ui.data.status == ecwui.InputStatus.ACTIVE:
        confirmed_obj["data"]["additions"].append(ui)
    elif ui.data.status == ecwui.InputStatus.DELETED:
        after_del_list = [ta for ta in confirmed_obj["data"]["additions"] if ta["data"]["match_id"] != ui["data"]["match_id"]]
        confirmed_obj["data"]["additions"] = after_del_list
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

    last_done_ts = None
    for ct in confirmedTrips:
        # Before confirmed_place was introduced, we created confirmed_trips without a confirmed_place
        # For those trips, we will generate a confirmed_place just-in-time and add its ID to the trip
        # Once every trip has a confirmed_place, we can remove this code
        if "confirmed_place" not in ct["data"]:
            cleaned_place = esda.get_entry(esda.CLEANED_PLACE_KEY, ct["data"]["end_place"])
            confirmed_place_entry = create_confirmed_place_entry(ts, cleaned_place)
            ts.insert(confirmed_place_entry)
            ct["data"]["confirmed_place"] = confirmed_place_entry["_id"]
            import emission.storage.timeseries.builtin_timeseries as estbt
            estbt.BuiltinTimeSeries.update(ct)

        logging.info("End place type for trip is %s" %  type(ct.data.end_place))
        composite_trip_dict = copy.copy(ct)
        del composite_trip_dict["_id"]
        composite_trip_dict["metadata"]["key"] = "analysis/composite_trip"

        # confirmed_trip has an id for its corresponding confirmed_place
        # for composite_trip, we want to get the actual confirmed_place object
        confirmed_place_id = ct["data"]["confirmed_place"]
        confirmed_place = esda.get_entry(esda.CONFIRMED_PLACE_KEY, confirmed_place_id)
        composite_trip_dict["data"]["confirmed_place"] = confirmed_place

        # retrieve locations for the trajectory of the trip
        time_query = estt.TimeQuery("data.ts", ct["data"]["start_ts"], ct["data"]["end_ts"])
        locations = esda.get_entries(esda.CLEANED_LOCATION_KEY, user_id, time_query=time_query)
        max_entries = 100; # we will downsample to 100 locations
        if len(locations) > max_entries:
            logging.debug('Downsampling to %d points' % max_entries)
            sample_rate = len(locations)//max_entries + 1
            locations = locations[::sample_rate]
        composite_trip_dict["data"]["locations"] = locations

        # later we will want to put section & modes in composite_trip as well

        composite_trip_entry = ecwe.Entry(composite_trip_dict)
        # save the entry
        ts.insert(composite_trip_entry)
        last_done_ts = confirmed_place["data"]["enter_ts"]

    epq.mark_composite_object_creation_done(user_id, last_done_ts)

def create_confirmed_objects(user_id):
    time_query = epq.get_time_range_for_confirmed_object_creation(user_id)
    try:
        # we will query the same time range for trips and places,
        # but querying 'enter_ts' for places and 'end_ts' for trips
        time_query.timeType = "data.enter_ts"
        processed_places = create_confirmed_places(user_id, time_query)
        time_query.timeType = "data.end_ts"
        last_expected_trip_done = create_confirmed_trips(user_id, time_query, processed_places)
        if last_expected_trip_done is None:
            logging.debug("after run, last_expected_trip_done == None, must be early return")
            epq.mark_confirmed_object_creation_done(user_id, None)
        else:
            epq.mark_confirmed_object_creation_done(user_id, last_expected_trip_done.data.end_ts)

    except:
        logging.exception("Error while creating confirmed objects, timestamp is unchanged")
        epq.mark_confirmed_object_creation_failed(user_id)

def create_confirmed_place_entry(ts, tcp):
    input_key_list = eac.get_config()["userinput.keylist"]
    # Copy the place and fill in the new values
    confirmed_place_dict = copy.copy(tcp)
    del confirmed_place_dict["_id"]
    confirmed_place_dict["metadata"]["key"] = "analysis/confirmed_place"
    confirmed_place_dict["data"]["cleaned_place"] = tcp.get_id()
    confirmed_place_dict["data"]["user_input"] = \
        get_user_input_dict(ts, tcp, input_key_list)
    confirmed_place_dict["data"]["additions"] = \
        esdt.get_additions_for_timeline_entry_object(ts, tcp)
    return ecwe.Entry(confirmed_place_dict)

def create_confirmed_places(user_id, timerange):
    ts = esta.TimeSeries.get_time_series(user_id)
    toConfirmPlaces = esda.get_entries(esda.CLEANED_PLACE_KEY, user_id,
        time_query=timerange)
    logging.info("Converting %d cleaned places to confirmed ones" % len(toConfirmPlaces))
    processed_places = []
    if len(toConfirmPlaces) == 0:
        logging.debug("len(toConfirmPlaces) == 0, early return")
        return None
    for tcp in toConfirmPlaces:
        confirmed_place_entry = create_confirmed_place_entry(ts, tcp)
        # save the entry
        ts.insert(confirmed_place_entry)
        # if everything is successful, then update the last successful place
        processed_places.append(confirmed_place_entry)

    return processed_places

def create_confirmed_trips(user_id, timerange, processed_places):
    ts = esta.TimeSeries.get_time_series(user_id)
    toConfirmTrips = esda.get_entries(esda.EXPECTED_TRIP_KEY, user_id,
        time_query=timerange)
    logging.debug("Converting %d expected trips to confirmed ones" % len(toConfirmTrips))
    lastTripProcessed = None
    if len(toConfirmTrips) == 0:
        logging.debug("len(toConfirmTrips) == 0, early return")
        return None
    input_key_list = eac.get_config()["userinput.keylist"]
    for i, tct in enumerate(toConfirmTrips):
        # Copy the trip and fill in the new values
        confirmed_trip_dict = copy.copy(tct)
        del confirmed_trip_dict["_id"]
        confirmed_trip_dict["metadata"]["key"] = "analysis/confirmed_trip"
        confirmed_trip_dict["data"]["expected_trip"] = tct.get_id()
        confirmed_trip_dict["data"]["confirmed_place"] = processed_places[i]["_id"]
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
            if ikey_name == "trip_user_input" or ikey_name == "place_user_input":
                tct_userinput[ikey_name] = matched_userinput
            else:
                tct_userinput[ikey_name] = matched_userinput.data.label
    logging.debug("for trip %s, returning user input dict %s" % (tct.get_id(), tct_userinput))
    return tct_userinput
