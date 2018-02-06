from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import *

import bson.json_util as bju
import requests
import json
import logging
import arrow

# Request data for the specified phones and time range
def request_data(server_url, from_ts, to_ts, phone_id, key_list, debug):
    url = server_url + "/datastreams/find_entries/time_type"
    request_body = {
        "user": phone_id,
        "key_list": key_list,
        "start_time": from_ts,
        "end_time": to_ts
    }
    headers = {'Content-Type': 'application/json'}

    r = requests.post(url, data=json.dumps(request_body), headers=headers)

    r.raise_for_status()

    dic = json.loads(r.text, object_hook=bju.object_hook)
    entry_list = dic['phone_data']

    if debug:
        logging.debug("first entry (in local time):")

        if len(entry_list) == 0:
            logging.debug("...has no data...")
        else:
            logging.debug(str(
                entry_list[0].get('metadata').get('write_fmt_time')))

    logging.debug("returning %d entries for batch %s (%s) -> %s (%s)" % 
        (len(entry_list),
        arrow.get(from_ts).to('local'), from_ts,
        arrow.get(to_ts).to('local'), to_ts))

    return entry_list

# Request data in 5hr-long chunks
def request_batched_data(server_url, from_ts, to_ts, phone_id, key_list):
    logging.info("Pulling batched data from %s (%s) -> %s (%s) for phone %s, key_list %s" %
        (arrow.get(from_ts).to('local'), from_ts,
         arrow.get(to_ts).to('local'), to_ts,
         phone_id, key_list))
    t1 = from_ts
    debug = True  # only set to True for the first loop iteration
    all_entries = []
    while t1 < to_ts:
        t2 = min(t1 + 5 * 60 * 60, to_ts)
        logging.info("Processing batch from %s (%s) -> %s (%s)" %
            (arrow.get(t1).to('local'), t1,
             arrow.get(t2).to('local'), t2))
        curr_batch = request_data(server_url, t1, t2, phone_id, key_list, debug)
        all_entries.extend(curr_batch)
        t1 = t2
        debug = False
    logging.info("Returning combined data of size = %s, first entries = %s" %
        (len(all_entries), all_entries[:3]))
    return all_entries
