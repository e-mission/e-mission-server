from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

import emission.storage.timeseries.abstract_timeseries as esta
import emission.core.wrapper.entry as ecwe

def get_last_entry(user_id, time_query, config_key):
    user_ts = esta.TimeSeries.get_time_series(user_id)
    
    # get the list of overrides for this time range. This should be non zero
    # only if there has been an override since the last run, which needs to be
    # saved back into the cache.
    config_overrides = list(user_ts.find_entries([config_key], time_query))
    logging.debug("Found %d user overrides for user %s" % (len(config_overrides), user_id))
    if len(config_overrides) == 0:
        logging.warning("No user defined overrides for key %s and user %s, early return" % (config_key, user_id))
        return (None, None)
    else:
        # entries are sorted by the write_ts, we can take the last value
        coe = ecwe.Entry(config_overrides[-1])
        logging.debug("last entry is %s" % coe)
        return (coe.data, coe.metadata.write_ts)
