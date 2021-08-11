import logging
import copy

import emission.storage.pipeline_queries as epq
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.core.wrapper.entry as ecwe
import emission.storage.timeseries.abstract_timeseries as esta
import emission.analysis.configs.expectation_notification_config as eace

# These may be altered to run tests
_test_options = {
    "preprocess_trip": None  # Runs on each trip before we process it, to do things like add fake end dates
}

# For a given user, determines which trips the user is expected to label (regardless of whether they have already done so) and marks them as such
def populate_expectations(user_id):
    last_trip_done = None
    time_range = epq.get_time_range_for_expectation_population(user_id)
    toPredictTrips = esda.get_entries(esda.INFERRED_TRIP_KEY, user_id, time_query=time_range)
    ts = esta.TimeSeries.get_time_series(user_id)

    for inferred_trip in toPredictTrips:
        expected_trip = _process_and_save_trip(user_id, inferred_trip, ts)
        if last_trip_done is None or last_trip_done["data"]["end_ts"] < expected_trip["data"]["end_ts"]:
                last_trip_done = expected_trip
    
    try:
        if last_trip_done is None:
            logging.debug("After run, last_trip_done == None, must be early return")
        epq.mark_expectation_population_done(user_id, last_trip_done)
    except:
        logging.exception("Error while inferring labels, timestamp is unchanged")
        epq.mark_expectation_population_failed(user_id)

def _process_and_save_trip(user_id, inferred_trip, ts):
    inferred_trip_dict = copy.copy(inferred_trip)["data"]
    expected_trip = ecwe.Entry.create_entry(user_id, "analysis/expected_trip", inferred_trip_dict)
    if _test_options["preprocess_trip"] is not None: _test_options["preprocess_trip"](expected_trip)

    expectation = _get_expectation_for_trip(expected_trip)
    confidence_threshold = eace.get_confidence_threshold(expected_trip)
    # For now, I don't think it's necessary to save each expectation as its own database entry

    expected_trip["data"]["inferred_trip"] = inferred_trip.get_id()
    expected_trip["data"]["expectation"] = expectation
    expected_trip["data"]["confidence_threshold"] = confidence_threshold
    ts.insert(expected_trip)

# This is a placeholder. TODO: implement the real algorithm
def _get_expectation_for_trip(trip):
    raw_expectation = eace.get_expectation(trip)
    # For now, expect always labeling unless the config file specifies no labeling at all
    processed_expectation = not raw_expectation["type"] == "none"
    return {"to_label": processed_expectation}
