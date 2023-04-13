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
import emission.storage.decorations.place_queries as esdp

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
            update_confirmed_and_composite(confirmed_obj)
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
        timeline.fill_start_end_places()
        last_processed_ts = None
        if not timeline.is_empty():
            logging.debug("cleaned timeline has %s places and %s trips" %
                (len(timeline.places), len(timeline.trips)))
            last_confirmed_place = esdp.get_last_place_entry(esda.CONFIRMED_PLACE_KEY, user_id)
            confirmed_tl = create_and_link_timeline(ts, timeline, last_confirmed_place)

            if last_confirmed_place is not None:
                logging.debug("last confirmed_place %s was already in database, updating with linked trip info... and %s additions" %
                    (last_confirmed_place["_id"], len(last_confirmed_place["data"]["additions"])))
                update_confirmed_and_composite(last_confirmed_place)

            if confirmed_tl is not None and not confirmed_tl.is_empty():
                ts.bulk_insert(list(confirmed_tl), esta.EntryType.ANALYSIS_TYPE)

            last_processed_ts = timeline.last_place().data.enter_ts

        epq.mark_confirmed_object_creation_done(user_id, last_processed_ts)
    except:
        logging.exception("Error while creating confirmed objects, timestamp is unchanged")
        epq.mark_confirmed_object_creation_failed(user_id)

def create_and_link_timeline(ts, timeline, last_confirmed_place):
    keys = eac.get_config()["userinput.keylist"]
    confirmed_places = []
    curr_confirmed_start_place = last_confirmed_place
    if last_confirmed_place is None:
        # If it is not present - maybe this user is getting started for the first
        # time, we create an entry based on the first trip from the timeline
        curr_confirmed_start_place = create_confirmed_entry(ts,
            timeline.first_place(), esda.CONFIRMED_PLACE_KEY, keys)
        logging.debug("no last confirmed place found, created place with id %s" %
            curr_confirmed_start_place.get_id())
        confirmed_places.append(curr_confirmed_start_place)
    else:
        # we update it with the information from the matching cleaned place
        matching_cleaned_place = timeline.first_place()
        curr_confirmed_start_place["data"]["exit_ts"] = matching_cleaned_place.data.exit_ts
        curr_confirmed_start_place["data"]["exit_fmt_time"] = matching_cleaned_place.data.exit_fmt_time
        curr_confirmed_start_place["data"]["exit_local_dt"] = matching_cleaned_place.data.exit_local_dt
        curr_confirmed_start_place["data"]["user_input"] = \
            get_user_input_dict(ts, matching_cleaned_place, keys)
        curr_confirmed_start_place["data"]["additions"] = \
            esdt.get_additions_for_timeline_entry_object(ts, matching_cleaned_place)
        logging.debug("Found existing last confirmed place, setting exit information to %s, and trimming additions to %s" %
            (matching_cleaned_place.data.exit_fmt_time, len(curr_confirmed_start_place["data"]["additions"])))

    if curr_confirmed_start_place is None:
        logging.debug("did not find any cleaned place either, early return")
        return None

    confirmed_trips = []

    logging.debug("Iterating through %s cleaned trips to create confirmed trips" % len(timeline.trips))
    for ctrip in timeline.trips:
        # create the trip-like object
        logging.debug("trip-like entry = %s" % ctrip)
        if ctrip['metadata']['key'] == esda.EXPECTED_TRIP_KEY:
            curr_confirmed_trip = create_confirmed_entry(ts, ctrip, esda.CONFIRMED_TRIP_KEY, keys)
        elif ctrip['metadata']['key'] == esda.CLEANED_UNTRACKED_KEY:
            curr_confirmed_trip = create_confirmed_entry(ts, ctrip, esda.CONFIRMED_UNTRACKED_KEY, keys)

        confirmed_trips.append(curr_confirmed_trip)

        # update the starting place
        link_trip_start(ts, curr_confirmed_trip, curr_confirmed_start_place)

        # create a new ending place
        cleaned_end_place = timeline.get_object(ctrip.data.end_place)
        curr_confirmed_end_place = create_confirmed_entry(ts, cleaned_end_place, esda.CONFIRMED_PLACE_KEY, keys)
        confirmed_places.append(curr_confirmed_end_place)
        link_trip_end(curr_confirmed_trip, curr_confirmed_end_place)
        curr_confirmed_start_place = curr_confirmed_end_place

    logging.debug("Creating confirmed timeline with %s places and %s trips" %
        (len(confirmed_places), len(confirmed_trips)))
    confirmed_tl = esdtl.Timeline(esda.CONFIRMED_PLACE_KEY,
                                    esda.CONFIRMED_TRIP_KEY,
                                    confirmed_places, confirmed_trips)
    return confirmed_tl


def create_confirmed_entry(ts, tce, confirmed_key, input_key_list):
    # Copy the entry and fill in the new values
    confirmed_object_data = copy.copy(tce["data"])
    # del confirmed_object_dict["_id"]
    # confirmed_object_dict["metadata"]["key"] = confirmed_key
    if (confirmed_key == esda.CONFIRMED_TRIP_KEY):
        confirmed_object_data["expected_trip"] = tce.get_id()
    elif (confirmed_key == esda.CONFIRMED_PLACE_KEY):
        confirmed_object_data["cleaned_place"] = tce.get_id()
    confirmed_object_data["user_input"] = \
        get_user_input_dict(ts, tce, input_key_list)
    confirmed_object_data["additions"] = \
        esdt.get_additions_for_timeline_entry_object(ts, tce)
    return ecwe.Entry.create_entry(tce['user_id'], confirmed_key, confirmed_object_data)

def get_confirmed_place_for_confirmed_trip(ct, place_key):
    # confirmed_trip already has an id for its confirmed_place we can lookup
    confirmed_place_id = ct["data"][place_key]
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

## Since there is no squishing, and the timestamps are the same as the underlying
# cleaned objects, the link code is much simpler than CLEAN_AND_RESAMPLE
# for now
def link_trip_start(ts, confirmed_trip, confirmed_start_place):
    logging.debug("for trip %s start, linking to %s" %
                  (confirmed_trip, confirmed_start_place))
    confirmed_trip["data"]["start_place"] = confirmed_start_place.get_id()
    confirmed_start_place["data"]["starting_trip"] = confirmed_trip.get_id()

def link_trip_end(confirmed_trip, confirmed_end_place):
    confirmed_trip["data"]["end_place"] = confirmed_end_place.get_id()
    confirmed_end_place["data"]["ending_trip"] = confirmed_trip.get_id()

def update_confirmed_and_composite(confirmed_obj):
    import emission.storage.timeseries.builtin_timeseries as estbt
    import emission.core.get_database as edb
    estbt.BuiltinTimeSeries.update(confirmed_obj)
    # we update confirmed object in the pipeline, but they are no longer the terminal data structures
    # that we display. So when the confirmed objects change, we need to find the corresponding
    # terminal objects (composite trips) and update them as well
    # if we don't find a matching composite trip, we don't need to do anything
    # since it has not been created yet and will be created with updated values when we get to that stage
    if confirmed_obj["metadata"]["key"] in [esda.CONFIRMED_TRIP_KEY, esda.CONFIRMED_UNTRACKED_KEY]:
        composite_trip = edb.get_analysis_timeseries_db().find_one({"data.confirmed_trip": confirmed_obj.get_id()})
        if composite_trip is not None:
            # copy over all the fields other than the end_confimed_place
            EXCLUDED_FIELDS = ["end_confirmed_place"]
            for k in confirmed_obj["data"].keys():
                if k not in EXCLUDED_FIELDS:
                    composite_trip["data"][k] = confirmed_obj["data"][k]
            estbt.BuiltinTimeSeries.update(ecwe.Entry(composite_trip))
        else:
            logging.debug("No composite trip matching confirmed trip %s, nothing to update" % confirmed_obj["_id"])

    if confirmed_obj["metadata"]["key"] == esda.CONFIRMED_PLACE_KEY:
        composite_trip = edb.get_analysis_timeseries_db().find_one({"data.end_confirmed_place._id": confirmed_obj["_id"]})
        if composite_trip is not None:
            composite_trip["data"]["end_confirmed_place"] = confirmed_obj
            estbt.BuiltinTimeSeries.update(ecwe.Entry(composite_trip))
        else:
            logging.debug("No composite trip ends at place %s, nothing to update" % confirmed_obj["_id"])
