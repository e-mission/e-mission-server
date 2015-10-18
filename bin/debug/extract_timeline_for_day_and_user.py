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

def export_timeline(user_id_str, day_str, file_name):
    logging.info("Extracting timeline for user %s day %s and saving to file %s" %
                 (user_id_str, day_str, file))

    # day_dt = pydt.datetime.strptime(day_str, "%Y-%m-%d").date()
    day_dt = pydt.datetime.strptime(day_str, "%Y-%m-%d")
    logging.debug("day_dt is %s" % day_dt)
    day_end_dt = day_dt + pydt.timedelta(days=1)
    # TODO: Convert to call to get_timeseries once we get that working
    # Or should we even do that?
    entry_list = list(edb.get_timeseries_db().find({'user_id': uuid.UUID(user_id_str),
                                                    'metadata.write_local_dt': {'$gt': day_dt, "$lt": day_end_dt}}))
    logging.info("Found %d entries" % len(entry_list))
    json.dump(entry_list, open(file_name, "w"), default=bju.default, allow_nan=False, indent=4)

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print "Usage: %s <user> <day> <file>" % (sys.argv[0])
    else:
        export_timeline(user_id_str=sys.argv[1], day_str=sys.argv[2], file_name=sys.argv[3])
