from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
# Exports all data for the particular user for the particular day
# Used for debugging issues with trip and section generation
from future import standard_library
standard_library.install_aliases()
from builtins import *
import sys
import logging
logging.basicConfig(level=logging.DEBUG)
import gzip

import uuid
import datetime as pydt
import json
import bson.json_util as bju
import arrow
import argparse

import emission.core.wrapper.user as ecwu
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt
import emission.storage.decorations.user_queries as esdu
import emission.storage.timeseries.cache_series as estcs
# only needed to read the motion_activity
# https://github.com/e-mission/e-mission-docs/issues/356#issuecomment-520630934
import emission.net.usercache.abstract_usercache as enua


def export(loc_time_query, trip_time_query, place_time_query, ma_entry_list, user_id, file_name):
	ts = esta.TimeSeries.get_time_series(user_id)
	loc_entry_list = list(estcs.find_entries(user_id, key_list=None, time_query=loc_time_query))
	trip_entry_list = list(ts.find_entries(key_list=None, time_query=trip_time_query))
	place_entry_list = list(ts.find_entries(key_list=None, time_query=place_time_query))
	first_place_extra_query = {'$and': [{'data.enter_ts': {'$exists': False}},
                                        {'data.exit_ts': {'$exists': True}}]}
    	first_place_entry_list = list(ts.find_entries(key_list=None, time_query=None, extra_query_list=[first_place_extra_query]))
    	logging.info("First place entry list = %s" % first_place_entry_list)
	combined_list = ma_entry_list + loc_entry_list + trip_entry_list + place_entry_list + first_place_entry_list
	
	logging.info("Found %d loc entries, %d motion entries, %d trip-like entries, %d place-like entries = %d total entries" %
        (len(loc_entry_list), len(ma_entry_list), len(trip_entry_list), len(place_entry_list), len(combined_list)))

    	validate_truncation(loc_entry_list, trip_entry_list, place_entry_list)

    	unique_key_list = set([e["metadata"]["key"] for e in combined_list])
    	logging.info("timeline has unique keys = %s" % unique_key_list)
    	if len(combined_list) == 0 or unique_key_list == set(['stats/pipeline_time']):
        	logging.info("No entries found in range for user %s, skipping save" % user_id)
    	else:
        	# Also dump the pipeline state, since that's where we have analysis results upto
        	# This allows us to copy data to a different *live system*, not just
        	# duplicate for analysis
        	combined_filename = "%s_%s.gz" % (file_name, user_id)
        	with gzip.open(combined_filename, "wt") as gcfd:
            		json.dump(combined_list,gcfd, default=bju.default, allow_nan=False, indent=4)

        	import emission.core.get_database as edb
        	pipeline_state_list = list(edb.get_pipeline_state_db().find({"user_id": user_id}))
        	logging.info("Found %d pipeline states %s" %
            		(len(pipeline_state_list),
             		list([ps["pipeline_stage"] for ps in pipeline_state_list])))

        	pipeline_filename = "%s_pipelinestate_%s.gz" % (file_name, user_id)
        	with gzip.open(pipeline_filename, "wt") as gpfd:
            		json.dump(pipeline_state_list,
                	gpfd, default=bju.default, allow_nan=False, indent=4)

def validate_truncation(loc_entry_list, trip_entry_list, place_entry_list):
    MAX_LIMIT = 25 * 10000
    if len(loc_entry_list) == MAX_LIMIT:
        logging.warning("loc_entry_list length = %d, probably truncated" % len(loc_entry_list))
    if len(trip_entry_list) == MAX_LIMIT:
        logging.warning("trip_entry_list length = %d, probably truncated" % len(trip_entry_list))
    if len(place_entry_list) == MAX_LIMIT:
        logging.warning("place_entry_list length = %d, probably truncated" % len(place_entry_list))
