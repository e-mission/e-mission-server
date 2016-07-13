import emission.core.get_database as edb
import bson.json_util as bju
import requests
import json
import argparse
import logging

parser = argparse.ArgumentParser()
parser.add_argument("from_date",
        help="from_date in the format of YYYY-MM-DD")
parser.add_argument("to_date",
        help="to_date in the format of YYYY-MM-DD")
parser.add_argument("server_url",
        help="url of the server to pull data from i.e. localhost:8080")
parser.add_argument("-v", "--verbose", 
		help="turn on debugging", action="store_true")

args = parser.parse_args()
from_date = args.from_date
to_date = args.to_date
server_url = args.server_url

if args.verbose:
	logging.basicConfig(level=logging.DEBUG)

r = requests.get("http://" + server_url + "/getData?from_date=" + from_date + "&to_date=" + to_date)

dic = r.json() 
iphone_list = dic['iphone_data'] 
android_list = dic['android_data']
phone_list = iphone_list + android_list 

tsdb = edb.get_timeseries_db()

print "Loading data from " + from_date + " to " + to_date 
print "..."

for index, entry_list in enumerate(phone_list):
	if index < 4:
		logging.debug("iphone" + str(index%4+1) + " first entry:")
	else:
		logging.debug("android" + str(index%4+1) + " first entry:")

	if len(entry_list) == 0:
		logging.debug("...has no data...")
	else:
		logging.debug(str(entry_list[0]))

	json_str = json.dumps(entry_list, default=bju.default, allow_nan=False, indent=4)
	entries = json.loads(json_str, object_hook = bju.object_hook)
	for entry in entries:
		tsdb.save(entry)

print "Data loaded to local server!"
