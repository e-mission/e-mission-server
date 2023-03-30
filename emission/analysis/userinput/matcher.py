import logging
import copy

# Get the configuration for the classifier
import emission.analysis.config as eac

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.timeline as esdtl
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

def create_confirmed_objects(user_id):
    tq = epq.get_time_range_for_confirmed_object_creation(user_id)
    try:
        ts = esta.TimeSeries.get_time_series(user_id)
        timeline = esdtl.get_timeline(user_id,
                           esda.CLEANED_PLACE_KEY,
                           esda.EXPECTED_TRIP_KEY,
                           esda.CLEANED_UNTRACKED_KEY,
                           tq.startTs, tq.endTs, trip_id_key='cleaned_trip')
        keys = eac.get_config()["userinput.keylist"]
        
        last_processed_ts = None
        cTrip = cPlace = untracked = False
        for tle in timeline:
            logging.debug("timeline entry = %s" % tle)
            if tle['metadata']['key'] == esda.CLEANED_PLACE_KEY:
                cPlace = create_confirmed_entry(ts, tle, esda.CONFIRMED_PLACE_KEY, keys)
            elif tle['metadata']['key'] == esda.EXPECTED_TRIP_KEY:
                cTrip = create_confirmed_entry(ts, tle, esda.CONFIRMED_TRIP_KEY, keys)
            elif tle['metadata']['key'] == esda.CLEANED_UNTRACKED_KEY:
                untracked = True

            if cPlace:
                logging.debug("Creating confirmed place")
                cp_id = ts.insert(cPlace)
                if cTrip:
                    logging.debug("Trip was before place, inserting confirmed trip with confirmed place id")
                    cTrip['data']['confirmed_place'] = cp_id
                    ts.insert(cTrip)
                elif untracked:
                    logging.debug("Untracked time was before place, confirmed trip will not be created")
                else:
                    logging.error("There was no triplike object before the place")
                last_processed_ts = cPlace['data']['enter_ts']
                cTrip = cPlace = untracked = False

        epq.mark_confirmed_object_creation_done(user_id, last_processed_ts)
    except:
        logging.exception("Error while creating confirmed objects, timestamp is unchanged")
        epq.mark_confirmed_object_creation_failed(user_id)

def create_confirmed_entry(ts, tce, confirmed_key, input_key_list):
    # Copy the entry and fill in the new values
    confirmed_trip_dict = copy.copy(tce)
    del confirmed_trip_dict["_id"]
    confirmed_trip_dict["metadata"]["key"] = confirmed_key
    if (confirmed_key == esda.CONFIRMED_TRIP_KEY):
        confirmed_trip_dict["data"]["expected_trip"] = tce.get_id()
    elif (confirmed_key == esda.CONFIRMED_PLACE_KEY):
        confirmed_trip_dict["data"]["cleaned_place"] = tce.get_id()
    confirmed_trip_dict["data"]["user_input"] = \
        get_user_input_dict(ts, tce, input_key_list)
    confirmed_trip_dict["data"]["additions"] = \
        esdt.get_additions_for_timeline_entry_object(ts, tce)
    return ecwe.Entry(confirmed_trip_dict)

def get_confirmed_place_for_confirmed_trip(ct):
    # confirmed_trip already has an id for its confirmed_place we can lookup
    confirmed_place_id = ct["data"]["confirmed_place"]
    return esda.get_entry(esda.CONFIRMED_PLACE_KEY, confirmed_place_id)

def get_user_input_dict(ts, tct, input_key_list):
    tct_userinput = {}
    for ikey in input_key_list:
        matched_userinput = esdt.get_user_input_for_timeline_entry(ts, tct, ikey)
        if matched_userinput is not None:
            ikey_name = obj_to_dict_key(ikey)
            if ikey_name == "trip_user_input" or ikey_name == "place_user_input":
                tct_userinput[ikey_name] = matched_userinput
            else:
                tct_userinput[ikey_name] = matched_userinput.data.label
    logging.debug("for trip %s, returning user input dict %s" % (tct.get_id(), tct_userinput))
    return tct_userinput
