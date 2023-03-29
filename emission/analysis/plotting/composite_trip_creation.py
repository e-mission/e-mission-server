import emission.core.wrapper.entry as ecwe
import emission.analysis.userinput.matcher as eaum
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.pipeline_queries as epq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt

import copy
import logging


def create_composite_trip(ts, ct):
    isUntrackedTime = ct["metadata"]["key"] == esda.CLEANED_UNTRACKED_KEY
    # Before confirmed_place was introduced, we created confirmed_trips without a confirmed_place
    # For those trips, we will generate a confirmed_place just-in-time and add its ID to the trip
    # Once every trip has a confirmed_place, we can remove this code
    if not isUntrackedTime and "confirmed_place" not in ct["data"]:
        cleaned_place = esda.get_entry(esda.CLEANED_PLACE_KEY, ct["data"]["end_place"])
        confirmed_place_entry = eaum.create_confirmed_place_entry(ts, cleaned_place)
        cpeid = ts.insert(confirmed_place_entry)
        ct["data"]["confirmed_place"] = cpeid
        logging.debug("Setting the confirmed_place key to the newly created id %s" % cpeid)
        import emission.storage.timeseries.builtin_timeseries as estbt
        estbt.BuiltinTimeSeries.update(ct)

    logging.info("End place type for trip is %s" % type(ct['data']['end_place']))
    composite_trip_dict = copy.copy(ct)
    del composite_trip_dict["_id"]
    composite_trip_dict["metadata"]["origin_key"] = ct["metadata"]["key"]
    composite_trip_dict["metadata"]["key"] = "analysis/composite_trip"
    composite_trip_dict["data"]["locations"] = get_locations_for_confirmed_trip(ct)
    if not isUntrackedTime:
        composite_trip_dict["data"]["confirmed_place"] = eaum.get_confirmed_place_for_confirmed_trip(ct)
    # later we will want to put section & modes in composite_trip as well
    composite_trip_entry = ecwe.Entry(composite_trip_dict)
    ts.insert(composite_trip_entry)

    return composite_trip_dict['data']['end_ts']


def create_composite_objects(user_id):
    time_query = epq.get_time_range_for_composite_object_creation(user_id)
    try:
        ts = esta.TimeSeries.get_time_series(user_id)
        # composite trips are created from both confirmed trips and cleaned untracked trips
        triplikeEntries = ts.find_entries([esda.CONFIRMED_TRIP_KEY, esda.CLEANED_UNTRACKED_KEY], time_query=time_query)
        last_done_ts = None
        if any(triplikeEntries):
            logging.debug("Creating composite trips from triplike entries")
            for t in triplikeEntries:
                last_done_ts = create_composite_trip(ts, t)
        else:
            logging.debug("No new triplikeEntries to process, timestamp is unchanged")
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
