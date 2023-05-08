from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import *
import argparse
import logging
import arrow 
from uuid import UUID
import json
import emission.storage.json_wrappers as esj

# This script pulls public data from the server and then loads it to a local server 
parser = argparse.ArgumentParser(prog="request_public_data")
parser.add_argument("from_date",
        help="from_date (local time, inclusive) in the format of YYYY-MM-DD-HH")
parser.add_argument("to_date",
        help="to_date (local time, exclusive) in the format of YYYY-MM-DD-HH")
parser.add_argument("server_url",
        help="url of the server to pull data from i.e. 'http://localhost:8080' or 'https://e-mission.eecs.berkeley.edu'")
parser.add_argument("phone_id",
        help="the phone id to pull data for i.e. 'ucb.sdb.android.1' or '4d21itu'")
parser.add_argument("key_list", metavar='key', nargs='+',
        help="the keys to pull data for i.e. 'background/battery' 'statemachine/transition'. Complete list is at https://github.com/e-mission/e-mission-server/blob/master/emission/core/wrapper/entry.py")

group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("-f", "--output_file",
    help="store to specified file")
group.add_argument("-d", "--database",
    help="store to local database", action='store_true')

parser.add_argument("-v", "--verbose", 
		help="turn on debugging", action="store_true")

args = parser.parse_args()
from_date = args.from_date
to_date = args.to_date
server_url = args.server_url

# Turn on logging if -v is specified 
if args.verbose:
	logging.basicConfig(level=logging.DEBUG)

# Time query range
from_ts = arrow.get(from_date, 'YYYY-MM-DD-HH').to('local').timestamp
to_ts = arrow.get(to_date, 'YYYY-MM-DD-HH').to('local').timestamp

logging.debug("from_ts = " + str(from_ts))
logging.debug("to_ts = " + str(to_ts))

# Pulling public data in batches 
print("Pulling data from " + from_date + " to " + to_date + " (local time)")
print("...")

import emission.public.pull_and_load_public_data as plpd
entries = plpd.request_batched_data(server_url, from_ts, to_ts, args.phone_id, args.key_list)
print("Retrieved %d entries starting with %s..." % (len(entries), entries[:3]))

import emission.storage.timeseries.abstract_timeseries as esta
if args.database:
    ts = esta.TimeSeries.get_time_series(args.phone_id)
    ts.bulk_insert(entries)
else:
    assert(args.output_file is not None)
    with open(args.output_file, "w") as fp:
        json.dump(entries, fp, default=esj.wrapped_default, allow_nan=False, indent=4)
