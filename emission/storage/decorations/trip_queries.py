from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import itertools
import logging
import pymongo
import arrow
import pandas as pd

import emission.storage.timeseries.timequery as estt

import emission.core.get_database as edb
import emission.core.wrapper.rawtrip as ecwrt
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.userinput as ecwui

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.cache_series as estsc
import emission.storage.decorations.timeline as esdt
import emission.storage.decorations.analysis_timeseries_queries as esda

EPOCH_MINIMUM = 0
EPOCH_MAXIMUM = 2**31 - 1

# helpers for getting start/enter and end/exit times of a trip/place
begin_of = lambda te: te['data'].get('start_ts', te['data'].get('enter_ts'))
end_of = lambda te: te['data'].get('end_ts', te['data'].get('exit_ts'))

def get_raw_sections_for_trip(user_id, trip_id):
    return get_sections_for_trip("segmentation/raw_section", user_id, trip_id)

def get_cleaned_sections_for_trip(user_id, trip_id):
    return get_sections_for_trip("analysis/cleaned_section", user_id, trip_id)

def get_raw_stops_for_trip(user_id, trip_id):
    return get_stops_for_trip("segmentation/raw_stop", user_id, trip_id)

def get_cleaned_stops_for_trip(user_id, trip_id):
    return get_stops_for_trip("analysis/cleaned_stop", user_id, trip_id)

def get_raw_timeline_for_trip(user_id, trip_id):
    """
    Get an ordered sequence of sections and stops corresponding to this trip.
    """
    return esdt.Timeline(esda.RAW_STOP_KEY, esda.RAW_SECTION_KEY,
                         get_raw_stops_for_trip(user_id, trip_id),
                         get_raw_sections_for_trip(user_id, trip_id))

def get_cleaned_timeline_for_trip(user_id, trip_id):
    """
    Get an ordered sequence of sections and stops corresponding to this trip.
    """
    return esdt.Timeline(esda.CLEANED_STOP_KEY, esda.CLEANED_SECTION_KEY,
                         get_cleaned_stops_for_trip(user_id, trip_id),
                         get_cleaned_sections_for_trip(user_id, trip_id))

def get_sections_for_trip(key, user_id, trip_id):
    # type: (UUID, object_id) -> list(sections)
    """
    Get the set of sections that are children of this trip.
    """
    query = {"user_id": user_id, "data.trip_id": trip_id,
             "metadata.key": key}
    logging.debug("About to execute query %s" % query)
    
    # Replace direct database calls with TimeSeries abstraction
    ts = esta.TimeSeries.get_time_series(user_id)
    # Don't include user_id in extra_query since it's already in the user_query
    extra_query = {k: v for k, v in query.items() 
                  if k != "metadata.key" and k != "user_id"}
    
    try:
        # Use metadata.write_ts for TimeQuery since all entries (including test data) have this field
        # This ensures we get all entries while still leveraging MongoDB sorting
        time_query = estt.TimeQuery("metadata.write_ts", 0, 9999999999)
        section_docs = ts.find_entries([key], time_query=time_query, extra_query_list=[extra_query])
        return [ecwe.Entry(doc) for doc in section_docs]
    except KeyError:
        # Return an empty list for invalid keys
        logging.warning(f"Invalid key {key} specified for get_sections_for_trip, returning empty list")
        return []

def get_stops_for_trip(key, user_id, trip_id):
    """
    Get the set of sections that are children of this trip.
    """
    query = {"user_id": user_id, "data.trip_id": trip_id,
             "metadata.key": key}
    logging.debug("About to execute query %s" % query)
    
    # Replace direct database calls with TimeSeries abstraction
    ts = esta.TimeSeries.get_time_series(user_id)
    # Don't include user_id in extra_query since it's already in the user_query
    extra_query = {k: v for k, v in query.items() 
                  if k != "metadata.key" and k != "user_id"}
    
    try:
        # Use metadata.write_ts for TimeQuery since all entries (including test data) have this field
        # This ensures we get all entries while still leveraging MongoDB sorting
        time_query = estt.TimeQuery("metadata.write_ts", 0, 9999999999)
        stop_docs = ts.find_entries([key], time_query=time_query, extra_query_list=[extra_query])
        return [ecwe.Entry(doc) for doc in stop_docs]
    except KeyError:
        # Return an empty list for invalid keys
        logging.warning(f"Invalid key {key} specified for get_stops_for_trip, returning empty list")
        return []

def _get_next_cleaned_timeline_entry(ts, tl_entry):
    """
    Find the next trip or place in the timeline
    """
    if ("end_place" in tl_entry.data):
        return ts.get_entry_from_id(esda.CLEANED_PLACE_KEY, tl_entry.data.end_place)
    elif ("starting_trip" in tl_entry.data):
        starting_trip = ts.get_entry_from_id(esda.CLEANED_TRIP_KEY, tl_entry.data.starting_trip)
        # if there is no cleaned trip, fall back to untracked time
        if starting_trip is None:
            logging.debug("Starting trip %s is not tracked, checking untracked time..." % tl_entry.data.starting_trip)
            starting_trip = ts.get_entry_from_id(esda.CLEANED_UNTRACKED_KEY, tl_entry.data.starting_trip)
        return starting_trip
    else:
        return None

def get_user_input_for_trip(trip_key, user_id, trip_id, user_input_key):
    ts = esta.TimeSeries.get_time_series(user_id)
    trip_obj = ts.get_entry_from_id(trip_key, trip_id)
    return get_user_input_for_timeline_entry(ts, trip_obj, user_input_key)

# Additional checks to be consistent with the phone code
# www/js/diary/services.js
# Since that has been tested the most
# If we no longer need these checks (maybe with trip editing), we can remove them
def valid_user_input_for_timeline_entry(ts, tl_entry, user_input):
    # we know that the trip is cleaned so we can use the fmt_time
    # but the confirm objects are not necessarily filled out
    fmt_ts = lambda ts, tz: arrow.get(ts).to(tz)

    entry_start = begin_of(tl_entry)
    entry_end = end_of(tl_entry)
    if entry_start is None:
        # a place will have no enter time if it is the first place in the timeline
        # so we will set the start time as low as possible for the purpose of comparison
        entry_start = EPOCH_MINIMUM
    if entry_end is None:
        # a place will have no exit time if the user hasn't left there yet
        # so we will set the end time as high as possible for the purpose of comparison
        entry_end = EPOCH_MAXIMUM

#     logging.warn("Comparing user input %s (%s) of type %s: %s -> %s, trip of type %s %s (%s) -> %s (%s)" %
#         (user_input.data.label, user_input.get_id(), user_input.metadata.key,
#         fmt_ts(user_input.data.start_ts, user_input.metadata.time_zone),
#         fmt_ts(user_input.data.end_ts, user_input.metadata.time_zone),
#         tl_entry.get_id(),
#         fmt_ts(entry_start, user_input.metadata.time_zone), entry_start,
#         fmt_ts(entry_end, user_input.metadata.time_zone), entry_end))

    logging.debug("Comparing user input %s: %s -> %s, trip %s -> %s, start checks are (%s && %s) and end checks are (%s || %s)" % (
        user_input.data.label,
        fmt_ts(user_input.data.start_ts, user_input.metadata.time_zone),
        fmt_ts(user_input.data.end_ts, user_input.metadata.time_zone),
        fmt_ts(entry_start, user_input.metadata.time_zone), fmt_ts(entry_end, user_input.metadata.time_zone),
        (user_input.data.start_ts >= entry_start),
        (user_input.data.start_ts < entry_end),
        (user_input.data.end_ts <= entry_end),
        ((user_input.data.end_ts - entry_end) <= 15 * 60)
    ))
    start_checks = (user_input.data.start_ts >= entry_start and
        user_input.data.start_ts < entry_end)
    end_checks = (user_input.data.end_ts <= entry_end or
        ((user_input.data.end_ts - entry_end) <= 15 * 60))
    if start_checks and not end_checks:
        logging.debug("Handling corner case where start check matches, but end check does not")
        next_entry_obj = _get_next_cleaned_timeline_entry(ts, tl_entry)
        if next_entry_obj is not None:
            next_entry_end = end_of(next_entry_obj)
            if next_entry_end is None: # the last place will not have an exit_ts
                end_checks = True # so we will just skip the end check
            else:
                end_checks = user_input.data.end_ts <= next_entry_end
                logging.debug("Second level of end checks when the next trip is defined (%s <= %s) = %s" % (
                    user_input.data.end_ts, next_entry_end, end_checks))
        else:
            end_checks = True
            logging.debug("Second level of end checks when the next trip is not defined = %s" % end_checks)
        if end_checks:
            # If we have flipped the values, check to see that there is sufficient overlap
            # https://github.com/e-mission/e-mission-docs/issues/476#issuecomment-747587041
            overlapDuration = min(user_input.data.end_ts, entry_end) - max(user_input.data.start_ts, entry_start)
            logging.debug("Flipped endCheck, overlap(%s)/trip(%s) = %s" %
                (overlapDuration, tl_entry.data.duration, (overlapDuration / tl_entry.data.duration)));
            end_checks = (overlapDuration/tl_entry.data.duration) > 0.5;
    return start_checks and end_checks

def valid_user_input(ts, trip_obj):
    def curried(user_input):
        return valid_user_input_for_timeline_entry(ts, trip_obj, user_input)
    return curried

def final_candidate(filter_fn, potential_candidates):
    potential_candidate_objects = [ecwe.Entry(c) for c in potential_candidates]
    extra_filtered_potential_candidates = list(filter(filter_fn, potential_candidate_objects))
    if len(extra_filtered_potential_candidates) == 0:
        return None

    # In general, all candiates will have the same start_ts, so no point in
    # sorting by it. Only exception to general rule is when user first provides
    # input before the pipeline is run, and then overwrites after pipeline is
    # run
    sorted_pc = sorted(extra_filtered_potential_candidates, key=lambda c:c["metadata"]["write_ts"])
    
    # for debug logs, we'll print out label if it exists; else use the start or enter time of the input
    entry_detail = lambda c: getattr(c.data, "label", \
                                getattr(c.data, "start_fmt_time", \
                                getattr(c.data, "enter_fmt_time", None)))
    logging.debug("sorted candidates are %s" %
        [{"write_fmt_time": c.metadata.write_fmt_time, "detail": entry_detail(c)} for c in sorted_pc])
    most_recent_entry = sorted_pc[-1]
    logging.debug("most recent entry is %s, %s" %
        (most_recent_entry.metadata.write_fmt_time, entry_detail(most_recent_entry)))
    return most_recent_entry

def get_not_deleted_candidates(filter_fn, potential_candidates):
    potential_candidate_objects = [ecwe.Entry(c) for c in potential_candidates]
    extra_filtered_potential_candidates = list(filter(filter_fn, potential_candidate_objects))
    if len(extra_filtered_potential_candidates) == 0:
        logging.debug(f"in get_not_deleted_candidates, no candidates, returning []")
        return []

    # We want to retain all ACTIVE entries that have not been DELETED
    all_active_list = [efpc for efpc in extra_filtered_potential_candidates if "status" not in efpc.data or efpc.data.status == ecwui.InputStatus.ACTIVE]
    all_deleted_id = [efpc["data"]["match_id"] for efpc in extra_filtered_potential_candidates if "status" in efpc.data and efpc.data.status == ecwui.InputStatus.DELETED]
    # TODO: Replace this with filter and a lambda if we decide not to match by ID after all
    not_deleted_active = [efpc for efpc in all_active_list if efpc["data"]["match_id"] not in all_deleted_id]
    logging.info(f"Found {len(all_active_list)} active entries, {len(all_deleted_id)} deleted entries -> {len(not_deleted_active)} non deleted active entries")
    return not_deleted_active

def get_time_query_for_timeline_entry(timeline_entry, force_start_end=True):
    begin_of_entry = begin_of(timeline_entry)
    end_of_entry = end_of(timeline_entry)
    inferred_time_type = lambda timeline_entry: "data.start_ts" if "start_ts" in timeline_entry.data else "data.enter_ts"
    timeType = "data.start_ts" if force_start_end else inferred_time_type
    if begin_of_entry is None:
        # a place will have no enter time if it is the first place in the timeline
        # so we will set the start time as low as possible for the purpose of comparison
        entry_start = EPOCH_MINIMUM
    if end_of_entry is None:
        # the last place (user's current place) will not have an exit_ts, so
        # every input from its enter_ts onward is fair game
        end_of_entry = EPOCH_MAXIMUM
    return estt.TimeQuery(timeType, begin_of_entry, end_of_entry)

def get_user_input_for_timeline_entry(ts, timeline_entry, user_input_key):
    # When we start supporting user inputs for places, we need to decide whether they will have
    # start/end or enter/exit. Depending on the decision, we can either remove support for
    # force_start_end (since we always use start/end) or pass in False (so we
    # use start/end or enter/exit appropriately)
    tq = get_time_query_for_timeline_entry(timeline_entry)
    potential_candidates = ts.find_entries([user_input_key], tq)
    return final_candidate(valid_user_input(ts, timeline_entry), potential_candidates)

# This is almost an exact copy of get_user_input_for_trip_object, but it
# retrieves an interable instead of a dataframe. So almost everything is
# different and it is hard to unify the implementations. Switching the existing
# function from get_data_df to find_entries may help us unify in the future

def get_user_input_from_cache_series(user_id, timeline_entry, user_input_key):
    # When we start supporting user inputs for places, we need to decide whether they will have
    # start/end or enter/exit. Depending on the decision, we can either remove support for
    # force_start_end (since we always use start/end) or pass in False (so we
    # use start/end or enter/exit appropriately)
    ts = esta.TimeSeries.get_time_series(user_id)
    tq = get_time_query_for_timeline_entry(timeline_entry)
    potential_candidates = estsc.find_entries(user_id, [user_input_key], tq)
    return final_candidate(valid_user_input(ts, timeline_entry), potential_candidates)

def get_additions_for_timeline_entry_object(ts, timeline_entry):
    addition_keys = ["manual/trip_addition_input", "manual/place_addition_input"]
    # This should always be start/end
    # https://github.com/e-mission/e-mission-docs/issues/880#issuecomment-1509875714
    tq = get_time_query_for_timeline_entry(timeline_entry)
    potential_candidates = ts.find_entries(addition_keys, tq)
    return get_not_deleted_candidates(valid_user_input(ts, timeline_entry), potential_candidates)

def valid_timeline_entry(ts, user_input):
    def curried(confirmed_obj):
        return valid_user_input_for_timeline_entry(ts, confirmed_obj, user_input)
    return curried

# Find the trip or place that the user input belongs to to
def get_confirmed_obj_for_user_input_obj(ts, ui_obj):
    # the match check that we have is:
    # user input can start after trip/place start
    # user input can end before trip/place end OR within 15 minutes after
    # Given those considerations, there is no principled query for trip/place data
    # that fits into our query model
    # the trip/place start is before the user input start, but that can go until eternity
    # and the trip/place end can be either before or after the user input end
    # we know that the trip/place end is after the user input start, but again, that
    # can go on until now.
    # As a workaround, let us assume that the trip/place start is no more than a day
    # before the start of the ui object, which seems like a fairly conservative
    # assumption
    ONE_DAY = 24 * 60 * 60
    tq = estt.TimeQuery("data.start_ts", ui_obj.data.start_ts - ONE_DAY,
        ui_obj.data.start_ts + ONE_DAY)
    
    # iff the input's key is one of these, the input belongs on a place
    # all other keys are only used for trip inputs
    place_keys = ["manual/place_user_input", "manual/place_addition_input"]
    if ui_obj['metadata']['key'] in place_keys:
        # if place, we'll query the same time range, but with 'enter_ts'
        tq.timeType = "data.enter_ts"
        potential_candidates = ts.find_entries(["analysis/confirmed_place"], tq)
    else:
        potential_candidates = ts.find_entries(["analysis/confirmed_trip"], tq)

    return final_candidate(valid_timeline_entry(ts, ui_obj), potential_candidates)

def filter_labeled_trips(mixed_trip_df):
    """
    mixed_trip_df: a dataframe with mixed labeled and unlabeled entries
    Returns only the labeled entries
    """
    if len(mixed_trip_df) == 0:
        return mixed_trip_df
    labeled_ct = mixed_trip_df[mixed_trip_df.user_input != {}]
    logging.debug("After filtering, found %s labeled trips" % len(labeled_ct))
    logging.debug(labeled_ct.head())
    return labeled_ct

def expand_userinputs(labeled_ct):
    """
    labeled_ct: a dataframe that contains potentially mixed trips.
    Returns a dataframe with the labels expanded into the main dataframe
    If the labels are simple, single level kv pairs (e.g. {mode_confirm:
    bike}), the expanded columns can be indexed very simply, like the other
    columns in the dataframe. Trips without labels are represented by N/A

    TODO: Replace by pandas.io.json.json_normalize?
    TODO: Currently duplicated from 
    https://github.com/e-mission/em-public-dashboard/blob/main/viz_scripts/scaffolding.py

    Should remove it from there
    """
    if len(labeled_ct) == 0:
        return labeled_ct
    label_only = pd.DataFrame(labeled_ct.user_input.to_list(), index=labeled_ct.index)
    logging.debug(label_only.head())
    expanded_ct = pd.concat([labeled_ct, label_only], axis=1)
    assert len(expanded_ct) == len(labeled_ct), \
        ("Mismatch after expanding labels, expanded_ct.rows = %s != labeled_ct.columns %s" %
            (len(expanded_ct), len(labeled_ct)))
    logging.debug("After expanding, columns went from %s -> %s" %
        (len(labeled_ct.columns), len(expanded_ct.columns)))
    logging.debug(expanded_ct.head())
    return expanded_ct

def has_final_labels(confirmed_trip_data):
    return (confirmed_trip_data["user_input"] != {}
            or confirmed_trip_data["expectation"]["to_label"] == False)

# Create an alternate method to work on the dataframe column-wise
# instead of iterating over each individual row for improved performance
def has_final_labels_df(df):
    # print(df.expectation)
    # print(pd.DataFrame(df.expectation.to_list(), index=df.index))
    to_list_series = pd.DataFrame(df.expectation.to_list(), index=df.index).to_label
    return df[(df.user_input != {})
            | (to_list_series == False)]

def get_max_prob_label(inferred_label_list):
    # Two columns: "labels" and "p"
    label_prob_df = pd.DataFrame(inferred_label_list)
    # logging.debug(label_prob_df)
    # idxmax returns the index corresponding to the max data value in each column
    max_p_idx = label_prob_df.p.idxmax()
    # logging.debug(max_p_idx)
    # now we look up the labels for that index
    return label_prob_df.loc[max_p_idx].labels

def expand_finallabels(labeled_ct):
    """
    labeled_ct: a dataframe that contains potentially mixed trips.
    Returns a dataframe with the user input labels and the high confidence
    inferred labels expanded into the main dataframe.  If the labels are
    simple, single level kv pairs (e.g. {mode_confirm: bike}), the expanded columns
    can be indexed very simply, like the other columns in the dataframe. Trips
    without labels are represented by N/A
    """
    if len(labeled_ct) == 0:
        return labeled_ct
    user_input_only = pd.DataFrame(labeled_ct.user_input.to_list(), index=labeled_ct.index)
    # Drop entries that are blank so we don't end up with duplicate entries in the concatenated dataframe.
    # without this change, concat might involve N/A rows from user entries,
    # inserted because the index is specified manually
    # then if they have high confidence entries, we will end up with
    # duplicated entries for N/A and the yellow labels
    user_input_only.dropna('index', how="all", inplace=True)
    logging.debug("user_input_only %s" % user_input_only.head())

    # see testExpandFinalLabelsPandasFunctionsNestedPostFilter for a step by step
    # walkthrough of how this section works. Note that
    # testExpandFinalLabelsPandasFunctionsNestedPostFilter has an alternate
    # implementation that we don't choose because it generates a UserWarning

    # Note that we could have entries that have both user inputs and high
    # confidence inferred values. This could happen if the user chooses to go
    # into "All Labels" and label high-confidence values. That's why the
    # algorithm
    # https://github.com/e-mission/e-mission-docs/issues/688#issuecomment-1000981037
    # specifies that we look for inferred values only if the user input does
    # not exist
    expectation_expansion = pd.DataFrame(labeled_ct.expectation.to_list(), index=labeled_ct.index)
    high_confidence_no_userinput_df = labeled_ct[
        (labeled_ct.user_input == {}) & (expectation_expansion.to_label == False)
    ]
    high_confidence_no_userinput_df.dropna('index', how="all", inplace=True)
    if len(high_confidence_no_userinput_df) > 0:
        high_confidence_inferred_labels = high_confidence_no_userinput_df.inferred_labels
        high_confidence_max_p_inferred_labels = high_confidence_inferred_labels.apply(get_max_prob_label)
        high_confidence_max_p_inferred_labels_only = pd.DataFrame(
                high_confidence_max_p_inferred_labels.to_list(),
                index=high_confidence_inferred_labels.index)
        logging.debug("high confidence inferred %s" % high_confidence_max_p_inferred_labels_only.head())

        assert pd.Series(labeled_ct.loc[
                high_confidence_max_p_inferred_labels_only.index].user_input == {}).all(), \
            ("Did not filter out all user inputs before expanding high confidence labels %s" %
                labeled_ct.loc[high_confidence_max_p_inferred_labels_only.index].user_input)
    else:
        high_confidence_max_p_inferred_labels_only = pd.DataFrame()


    # see testExpandFinalLabelsPandasFunctions for a step by step walkthrough of this section
    naive_concat = pd.concat([user_input_only, high_confidence_max_p_inferred_labels_only], axis=0)
    # print(naive_concat)
    label_only = naive_concat.reindex(labeled_ct.index)

    expanded_ct = pd.concat([labeled_ct, label_only], axis=1)
    assert len(expanded_ct) == len(labeled_ct), \
        ("Mismatch after expanding labels, expanded_ct.rows = %s != labeled_ct.columns %s" %
            (len(expanded_ct), len(labeled_ct)))
    logging.debug("After expanding, columns went from %s -> %s" %
        (len(labeled_ct.columns), len(expanded_ct.columns)))
    logging.debug(expanded_ct.head())
    return expanded_ct
