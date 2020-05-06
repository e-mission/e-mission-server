from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Note that this script is only retained for historical purposes,
# to document how we expanded the local date entries. It will not run
# any more, since we have removed the trip, place, section and stop
# collections and merged them into the analysis database

from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
# logging.basicConfig(level=logging.DEBUG)

import arrow
import argparse
import json

import emission.core.get_database as edb
import emission.core.wrapper.localdate as ecwld

# For entries in the timeseries, this is simple because all of them follow the "ts" -> "local_dt" -> "fmt_time" pattern.
# We can just parse the fmt_time to get an arrow object and then get all the components
# For trips, sections, places and stops, we still have the fmt time, we just need to parse individual fields properly

# In order to allow us to run multiple processes in parallel, this takes the
# key of the stream as the input. Then we can run multiple processes, one for
# each stream, in parallel

def get_local_date(fmt_time, timezone):
    """
    When we parse the fmt time, we get a timezone offset, but not the timezone string.
    Timezone string seems more portable, so we want to use that instead.
    So we need to get it from somewhere else and pass it in here
    """
    adt = arrow.get(fmt_time)
    logging.debug("after parsing, adt = %s" % adt)
    return ecwld.LocalDate.get_local_date(adt.timestamp, timezone)
        
def fix_timeseries(key):
    tsdb = edb.get_timeseries_db()
    tsdb_cursor = tsdb.find({'metadata.key': key})
    logging.debug("Fixing %s entries for key %s" % (tsdb_cursor.count(), key))
    data_local_dt = False
    for entry in tsdb.find({'metadata.key': key}):
        entry["metadata"]["write_local_dt"] = get_local_date(entry['metadata']['write_fmt_time'],
            entry['metadata']['time_zone'])
        if 'local_dt' in entry['data']:
            if data_local_dt == False:
                logging.info("overriding local_dt for key %s" % key)
                data_local_dt = True
            entry['data']['local_dt'] = get_local_date(entry['data']['fmt_time'],
                entry['metadata']['time_zone'])
        else:
            if data_local_dt == True:
                logging.info("not overriding local_dt for key %s" % key)
                data_local_dt = False
        tsdb.save(entry)

def fix_file(filename):
    timeseries = json.load(open(filename))
    logging.debug("Fixing %s entries for filename %s" % (len(timeseries), filename))
    data_local_dt = False
    for entry in timeseries:
        entry["metadata"]["write_local_dt"] = get_local_date(entry['metadata']['write_fmt_time'],
            entry['metadata']['time_zone'])
        if 'local_dt' in entry['data']:
            if data_local_dt == False:
                logging.info("overriding local_dt for file %s" % filename)
                data_local_dt = True
            entry['data']['local_dt'] = get_local_date(entry['data']['fmt_time'],
                entry['metadata']['time_zone'])
        else:
            if data_local_dt == True:
                logging.info("not overriding local_dt for file %s" % filename)
                data_local_dt = False
    logging.debug("Finished converting %s entries" % len(timeseries))
    json.dump(timeseries, open(filename, "w"), indent=4)

def fix_trips_or_sections(collection):
    tsdb = edb.get_timeseries_db()
    for entry in collection.find():
        start_loc_entry = tsdb.find_one({'user_id': entry['user_id'],
            'metadata.key': 'background/location', 'data.ts': entry['start_ts']})
        end_loc_entry = tsdb.find_one({'user_id': entry['user_id'],
            'metadata.key': 'background/location', 'data.ts': entry['end_ts']})

        if start_loc_entry is not None:
            start_tz = start_loc_entry['metadata']['time_zone']
        else:
            logging.warn("No start_loc_entry found for trip %s, returning default" % entry)
            start_tz = "America/Los_Angeles"

        if end_loc_entry is not None:
            end_tz = end_loc_entry['metadata']['time_zone']
        else:
            logging.warn("No end_loc_entry found for trip %s, returning default" % entry)
            end_tz = "America/Los_Angeles"

        logging.debug("Found entries with metadata = %s, %s" % (start_tz, end_tz))

        entry['start_local_dt'] = get_local_date(entry['start_fmt_time'], start_tz)
        entry['end_local_dt'] = get_local_date(entry['end_fmt_time'], end_tz)

        collection.save(entry)

def fix_stops_or_places(collection):
    tsdb = edb.get_timeseries_db()
    for entry in collection.find():
        if 'enter_ts' in entry:
            enter_loc_entry = tsdb.find_one({'user_id': entry['user_id'],
                'metadata.key': 'background/location', 'data.ts': entry['enter_ts']})
            if enter_loc_entry is not None:
                enter_tz = enter_loc_entry['metadata']['time_zone']
            else:
                enter_tz = "America/Los_Angeles"
            logging.debug("entry metadata timezone = %s" % enter_tz)
            entry['enter_local_dt'] = get_local_date(entry['enter_fmt_time'],
                enter_tz)
        else:
            logging.warning("No entry timestamp found, skipping")
        
        if 'exit_ts' in entry:
            exit_loc_entry = tsdb.find_one({'user_id': entry['user_id'],
                'metadata.key': 'background/location', 'data.ts': entry['exit_ts']})
            if exit_loc_entry is not None:
                exit_tz = exit_loc_entry['metadata']['time_zone']
            else:
                exit_tz = "America/Los_Angeles"
            logging.debug("exit metadata timezone = %s" % exit_tz)
            entry['exit_local_dt'] = get_local_date(entry['exit_fmt_time'], exit_tz)
        else:
            logging.warning("No exit timestamp found, skipping")

        collection.save(entry)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("key",
        help="the key representing the stream that we want to fix")
    parser.add_argument("-f", "--filename",
        help="a saved timeline whose local_dt needs to be fixed. If this is specified, key is ignored")

    args = parser.parse_args()
    if args.filename is not None:
        fix_file(args.filename)
    elif args.key == "trips":
        fix_trips_or_sections(edb.get_trip_new_db())
    elif args.key == "sections":
        fix_trips_or_sections(edb.get_section_new_db())
    elif args.key == "places":
        fix_stops_or_places(edb.get_place_db())
    elif args.key == "stops":
        fix_stops_or_places(edb.get_stop_db())
    else:
        fix_timeseries(args.key)

