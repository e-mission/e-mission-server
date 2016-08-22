import emission.core.get_database as edb
import bson.json_util as bju
import requests
import json
import logging

# Request data for the specified phones and time range
def request_data(server_url, from_ts, to_ts, phone_ids, debug):
	url = server_url + "/eval/publicData/timeseries?from_ts=" + str(from_ts) + "&to_ts=" + str(to_ts)
	ids = {'phone_ids': phone_ids}
	headers = {'Content-Type': 'application/json'}

	r = requests.get(url, data=json.dumps(ids), headers = headers)

	dic = json.loads(r.text, object_hook = bju.object_hook)
	phone_list = dic['phone_data']

	if phone_list == None:
		print "Requested amount of data exceeds the threshold value."
	else:  
		# Load data to the local server 
		tsdb = edb.get_timeseries_db()

		for index, entry_list in enumerate(phone_list):
			if debug:
				logging.debug("phone" + str(index+1) + " first entry (in Pacific Time):")

				if len(entry_list) == 0:
					logging.debug("...has no data...")
				else:
					logging.debug(str(entry_list[0].get('metadata').get('write_fmt_time')))

			for entry in entry_list:
				tsdb.save(entry)


# Request data in 5hr-long chunks
def request_batched_data(server_url, from_ts, to_ts, phone_ids):
	t1 = from_ts
	debug = True # only set to True for the first loop iteration 
	while t1 < to_ts:
		t2 = min(t1 + 5*60*60, to_ts)
		request_data(server_url, t1, t2, phone_ids, debug)
		t1 = t2 
		debug = False
	print "Data loaded to local server!"
