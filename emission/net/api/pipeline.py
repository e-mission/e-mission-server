from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import pymongo

import emission.storage.pipeline_queries as esp
import emission.storage.timeseries.abstract_timeseries as esta

def get_complete_ts(user_id):
    complete_ts = esp.get_complete_ts(user_id)
    logging.debug("Returning complete_ts = %s" % complete_ts)
    return complete_ts

def get_range(user_id):
    ts = esta.TimeSeries.get_time_series(user_id)
    start_ts = ts.get_first_value_for_field("analysis/confirmed_trip", "data.start_ts", pymongo.ASCENDING)
    if start_ts == -1:
        start_ts = ts.get_first_value_for_field("analysis/cleaned_trip", "data.start_ts", pymongo.ASCENDING)
    if start_ts == -1:
        start_ts = None

    end_ts = ts.get_first_value_for_field("analysis/confirmed_trip", "data.end_ts", pymongo.DESCENDING)
    if end_ts == -1:
        end_ts = ts.get_first_value_for_field("analysis/cleaned_trip", "data.end_ts", pymongo.DESCENDING)
    if end_ts == -1:
        end_ts = None

    complete_ts = get_complete_ts(user_id)
    if complete_ts is not None and end_ts is not None\
        and (end_ts != (complete_ts - esp.END_FUZZ_AVOID_LTE)):
        logging.exception("end_ts %s != complete_ts no fuzz %s" %
            (end_ts, (complete_ts - esp.END_FUZZ_AVOID_LTE)))

    logging.debug("Returning range (%s, %s)" % (start_ts, end_ts))
    return (start_ts, end_ts)
