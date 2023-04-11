import emission.analysis.config as eac
import emission.core.wrapper.entry as ecwe
import emission.analysis.userinput.matcher as eaum
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.pipeline_queries as epq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt

import copy
import logging


def create_composite_trip(ts, ct):
    keys = eac.get_config()["userinput.keylist"]
    isUntrackedTime = ct["metadata"]["key"] == esda.CONFIRMED_UNTRACKED_KEY
    # Before all this place work, we created confirmed trips by copying from cleaned trips,
    # so the start and end places were cleaned places. We add in hack to
    # convert the cleaned places to confirmed places if they are "old style"
    # The hack can be removed in the point release since we would have
    # converted all trips by then
    # Note that there was originally a different hack in here that used the
    # presence or absence of the `confirmed_place` object for the conversion -
    # that was removed as inconsistent with the data model
    # https://github.com/e-mission/e-mission-docs/issues/880#issuecomment-1502015267
    import emission.core.get_database as edb
    curr_confirmed_place_count = edb.get_analysis_timeseries_db().count_documents({"metadata.key": "analysis/confirmed_place"})
    if "additions" in ct["data"] and "trip_addition" not in ct["data"]:
        logging.info("Most recent format, no need to convert")
        needs_hack = False
        assert curr_confirmed_place_count > 0
    elif "additions" in ct["data"] and ["trip_addition"] in ct["data"]:
        logging.info("Intermediate format, converting from cleaned to confirmed and removing trip_addition")
        needs_hack = True
        assert curr_confirmed_place_count == 0
        convert_cleaned_to_confirmed(ct)
        del ct["data"]["trip_addition"]
    else:
        assert "additions" not in ct["data"]
        logging.info("old-style format, converting from cleaned to confirmed")
        needs_hack = True
        assert curr_confirmed_place_count == 0
        convert_cleaned_to_confirmed(ct)
        ct["data"]["additions"] = []

    if needs_hack:
        import emission.storage.timeseries.builtin_timeseries as estbt
        estbt.BuiltinTimeSeries.update(ecwe.Entry(ct))

    logging.info("End place type for trip is %s" % type(ct['data']['end_place']))
    composite_trip_data = copy.copy(ct["data"])
    origin_key = ct["metadata"]["key"]
    logging.debug("Origin key for trip %s is %s" % (ct["_id"], origin_key))
    composite_trip_data["locations"] = get_locations_for_confirmed_trip(ct)
    # The place that follows untracked time has a duration of 0.
    # Thus, we are not going to consider it eligible for additions or user input,
    # and so untracked composite objects will not have a confirmed_place.
    if not isUntrackedTime:
        composite_trip_data["end_confirmed_place"] = eaum.get_confirmed_place_for_confirmed_trip(ct, "end_place")
    # later we will want to put section & modes in composite_trip as well
    composite_trip_entry = ecwe.Entry.create_entry(ct["user_id"], "analysis/composite_trip", composite_trip_data)
    composite_trip_entry["metadata"]["origin_key"] = origin_key
    ts.insert(composite_trip_entry)

    return composite_trip_data['end_ts']

def convert_cleaned_to_confirmed(ct):
    # most recent style, check for object type
    start_cleaned_place = esda.get_entry(esda.CLEANED_PLACE_KEY, ct["data"]["start_place"])
    if start_cleaned_place is None:
        logging.debug("start place is not a cleaned place, must be a confirmed place, skipping...")
    else:
        start_confirmed_place_entry = eaum.create_confirmed_entry(ts, start_cleaned_place, esda.CONFIRMED_PLACE_KEY, keys)
        start_cpeid = ts.insert(start_confirmed_place_entry)
        ct["data"]["start_place"] = start_cpeid
        logging.debug("Setting the start_place key to the newly created id %s" % cpeid)

    end_cleaned_place = esda.get_entry(esda.CLEANED_PLACE_KEY, ct["data"]["start_place"])
    if end_cleaned_place is None:
        logging.debug("start place is not a cleaned place, must be a confirmed place, skipping...")
    else:
        end_confirmed_place_entry = eaum.create_confirmed_entry(ts, start_cleaned_place, esda.CONFIRMED_PLACE_KEY, keys)
        end_cpeid = ts.insert(start_confirmed_place_entry)
        ct["data"]["end_place"] = start_cpeid
        logging.debug("Setting the end_place key to the newly created id %s" % cpeid)

    return (start_cleaned_place is not None) or (end_cleaned_place is not None)


def create_composite_objects(user_id):
    time_query = epq.get_time_range_for_composite_object_creation(user_id)
    try:
        ts = esta.TimeSeries.get_time_series(user_id)
        # composite trips are created from both confirmed trips and cleaned untracked trips
        triplikeEntries = ts.find_entries([esda.CONFIRMED_TRIP_KEY, esda.CONFIRMED_UNTRACKED_KEY], time_query=time_query)
        last_done_ts = None
        count_created = 0
        for t in triplikeEntries:
            last_done_ts = create_composite_trip(ts, t)
            count_created += 1
        logging.debug("Created %d composite trips" % count_created if count_created > 0
                      else "No new triplike entries to process, no composite trips created")
        epq.mark_composite_object_creation_done(user_id, last_done_ts)
    except:
        logging.exception("Error while creating composite objects, timestamp is unchanged")
        epq.mark_composite_object_creation_failed(user_id)


# retrieve locations for the trajectory of the trip
# downsampled to max_entries (default 100)
def get_locations_for_confirmed_trip(ct, max_entries=100):
    if ct["metadata"]["key"] == esda.CLEANED_UNTRACKED_KEY:
        return [] # untracked time has no locations
    # retrieve locations for the trajectory of the trip
    time_query = estt.TimeQuery("data.ts", ct["data"]["start_ts"], ct["data"]["end_ts"])
    locations = esda.get_entries(esda.CLEANED_LOCATION_KEY, ct['user_id'], time_query=time_query)
    if len(locations) > max_entries:
        logging.debug('Downsampling to %d points' % max_entries)
        sample_rate = len(locations)//max_entries + 1
        locations = locations[::sample_rate]
    return locations
