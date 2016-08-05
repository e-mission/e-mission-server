import emission.core.get_database as edb
import bson.json_util as bju
import requests
import json
import argparse
import logging
import arrow 
from uuid import UUID

# List of UUIDs of phones to pull data for 
iphone_ids = ["079e0f1a-c440-3d7c-b0e7-de160f748e35", "c76a0487-7e5a-3b17-a449-47be666b36f6", 
              "c528bcd2-a88b-3e82-be62-ef4f2396967a", "95e70727-a04e-3e33-b7fe-34ab19194f8b"]
android_ids = ["e471711e-bd14-3dbe-80b6-9c7d92ecc296", "fd7b4c2e-2c8b-3bfa-94f0-d1e3ecbd5fb7",
             "86842c35-da28-32ed-a90e-2da6663c5c73", "3bc0f91f-7660-34a2-b005-5c399598a369",
             "273efe85-937e-3622-9b34-19cb64653a9f"]
phone_ids = iphone_ids + android_ids

# This script pulls public data from the server and then loads it to a local server 
parser = argparse.ArgumentParser()
parser.add_argument("from_date",
        help="from_date (local time, inclusive) in the format of YYYY-MM-DD-HH")
parser.add_argument("to_date",
        help="to_date (local time, exclusive) in the format of YYYY-MM-DD-HH")
parser.add_argument("server_url",
        help="url of the server to pull data from i.e. 'localhost:8080' or 'e-mission.eecs.berkeley.edu'")
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
from_ts = arrow.get(from_date, 'YYYY-MM-DD-HH').replace(tzinfo='local').timestamp
to_ts = arrow.get(to_date, 'YYYY-MM-DD-HH').replace(tzinfo='local').timestamp

logging.debug("from_ts = " + str(from_ts))
logging.debug("to_ts = " + str(to_ts))

url = "http://" + server_url + "/eval/publicData/timeseries?from_ts=" + str(from_ts) + "&to_ts=" + str(to_ts)
ids = {'phone_ids': phone_ids}
headers = {'Content-Type': 'application/json'}

r = requests.get(url, data=json.dumps(ids), headers = headers)

print r 

dic = json.loads(r.text, object_hook = bju.object_hook)
phone_list = dic['phone_data']

tsdb = edb.get_timeseries_db()

print "Loading data from " + from_date + " to " + to_date + " (local time)"
print "..."

for index, entry_list in enumerate(phone_list):
	logging.debug("phone" + str(index+1) + " first entry:")

	if len(entry_list) == 0:
		logging.debug("...has no data...")
	else:
		logging.debug(str(entry_list[0]))

	for entry in entry_list:
		tsdb.save(entry)

print "Data loaded to local server!"


