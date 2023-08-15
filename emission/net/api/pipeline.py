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
import emission.core.wrapper.user as ecwu

def get_complete_ts(user_id):
    complete_ts = esp.get_complete_ts(user_id)
    logging.debug("Returning complete_ts = %s" % complete_ts)
    return complete_ts

def get_range(user_id):
    user_profile = ecwu.User(user_id).getProfile()
    start_ts = user_profile.get("pipeline_range", {}).get("start_ts", None)
    end_ts = user_profile.get("pipeline_range", {}).get("end_ts", None)
    logging.debug("Returning range (%s, %s)" % (start_ts, end_ts))
    return (start_ts, end_ts)
