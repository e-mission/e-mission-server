from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
import emission.storage.timeseries.abstract_timeseries as esta
import emission.net.ext_service.push.notify_usage as pnu
import emission.core.wrapper.user as ecwu
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import logging.config
import argparse
import pandas as pd
import requests
import json
import re
import emission.core.get_database as edb
from uuid import UUID


def handle_insert(tripDict, tripID, collection, uuid):
    if tripDict == None:
        collection.insert_one({'uuid': uuid, 'trip_id': tripID})
        return True
    else:
        if tripDict['trip_id'] != tripID:
            collection.update_one({'uuid': uuid}, {'$set': {'trip_id' : tripID}})
            return True
        else:
            return False

def calculate_single_yelp_suggestion(UUID):
	logging.debug("About to calculate single suggestion for %s" % UUID)
	yelp_suggestion_trips = edb.get_yelp_db()
	return_obj = {'message': "Good job walking and biking! No suggestion to show.",
    'savings': "0", 'method' : 'bike'}
    all_users = pd.DataFrame()
