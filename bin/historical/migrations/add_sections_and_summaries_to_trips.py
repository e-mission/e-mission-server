import logging
import arrow
import argparse

import emission.core.get_database as edb
import emission.core.wrapper.entry as ecwe

import emission.pipeline.scheduler as eps

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.user_queries as esdu
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.analysis.plotting.composite_trip_creation as eapc
import emission.analysis.userinput.matcher as eaum

def add_sections_to_trips(process_number, uuid_list):
    import logging
    logging.basicConfig(level=logging.DEBUG, filename="/var/tmp/add_section_summary_%s.log" % process_number,
        force=True)
    logging.info("processing UUID list = %s" % uuid_list)

    for uuid in uuid_list:
        if uuid is None:
            continue

        try:
            add_sections_to_trips_for_user(uuid)
        except Exception as e:
            print("Found error %s while processing pipeline for user %s, check log files for details"
                % (e, uuid))
            logging.exception("Found error %s while processing pipeline "
                              "for user %s, skipping" % (e, uuid))
    
def add_sections_to_trips_for_user(uuid):
    ts = esta.TimeSeries.get_time_series(uuid)
    cleaned_trips = list(ts.find_entries([esda.CLEANED_TRIP_KEY]))
    confirmed_trips = list(ts.find_entries([esda.CONFIRMED_TRIP_KEY]))
    composite_trips =  list(ts.find_entries([esda.COMPOSITE_TRIP_KEY]))
    cleaned_trips_map = dict((t["_id"], t) for t in cleaned_trips)
    composite_trips_map = dict((t["data"]["confirmed_trip"], t) for t in composite_trips)
    # This script is slow due to DB queries
    # so let's read all the trips upfront, and reuse the section information where possible
    # don't want to optimize too much, though, since this is a one-off script
    for t in confirmed_trips:
        matching_composite_trip = composite_trips_map[t["_id"]]
        matching_cleaned_trip = cleaned_trips_map[t["data"]["cleaned_trip"]]
        logging.debug("Processing confirmed trip %s with matching cleaned trip %s, and composite trip %s" %
            (t["_id"], matching_cleaned_trip["_id"], matching_composite_trip["_id"]))
        if "inferred_section_summary" not in t["data"]:
            # we need to add the summary to both the confirmed trip and the associated composite trip
            t["data"]['inferred_section_summary'] = eaum.get_section_summary(ts, matching_cleaned_trip, "analysis/inferred_section")
            t["data"]['cleaned_section_summary'] = eaum.get_section_summary(ts, matching_cleaned_trip, "analysis/cleaned_section")
            matching_composite_trip["data"]["inferred_section_summary"] = t["data"]["inferred_section_summary"]
            matching_composite_trip["data"]["cleaned_section_summary"] = t["data"]["cleaned_section_summary"]
            matching_composite_trip["data"]["sections"] = eapc.get_sections_for_confirmed_trip(t)

            import emission.storage.timeseries.builtin_timeseries as estbt
            estbt.BuiltinTimeSeries.update(ecwe.Entry(t))
            estbt.BuiltinTimeSeries.update(ecwe.Entry(matching_composite_trip))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("n_workers", type=int,
                        help="the number of worker processors to use")
    args = parser.parse_args()
    split_lists = eps.get_split_uuid_lists(args.n_workers)
    logging.info("Finished generating split lists %s" % split_lists)
    eps.dispatch(split_lists, target_fn=add_sections_to_trips)


