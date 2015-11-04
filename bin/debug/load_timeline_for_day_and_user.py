import json
import bson.json_util as bju
import emission.core.get_database as edb
import sys

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: %s <filename>" % (sys.argv[0])

    fn = sys.argv[1]
    print "Loading file " + fn
    entries = json.load(open(fn), object_hook = bju.object_hook)
    for entry in entries:
        edb.get_timeseries_db().save(entry)
