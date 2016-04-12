# Exports all data for the particular user for the particular day
# Used for debugging issues with trip and section generation 
import sys
import logging
logging.basicConfig(level=logging.DEBUG)
import uuid
import datetime as pydt
import json
import bson.json_util as bju

import emission.core.get_database as edb

def export_timeline(user_id_str, start_day_str, end_day_str, file_name):
    logging.info("Extracting trips for user %s day %s -> %s and saving to file %s" %
                 (user_id_str, start_day_str, end_day_str, file))

    # day_dt = pydt.datetime.strptime(day_str, "%Y-%m-%d").date()
    start_day_dt = pydt.datetime.strptime(start_day_str, "%Y-%m-%d")
    end_day_dt = pydt.datetime.strptime(end_day_str, "%Y-%m-%d")
    logging.debug("start_day_dt is %s, end_day_dt is %s" % (start_day_dt, end_day_dt))
    # TODO: Convert to call to get_timeseries once we get that working
    # Or should we even do that?
    query = {'user_id': uuid.UUID(user_id_str), 'start_local_dt': {'$gt': start_day_dt, "$lt": end_day_dt}}
    print "query = %s" % query
    entry_list = list(edb.get_trip_new_db().find(query))
    logging.info("Found %d entries" % len(entry_list))
    json.dump(entry_list, open(file_name, "w"), default=bju.default, allow_nan=False, indent=4)

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print "Usage: %s <user> <start_day> <end_day> <file>" % (sys.argv[0])
    else:
        export_timeline(user_id_str=sys.argv[1], start_day_str=sys.argv[2], end_day_str=sys.argv[3], file_name=sys.argv[4])
