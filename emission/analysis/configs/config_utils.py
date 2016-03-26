import logging

import emission.storage.timeseries.abstract_timeseries as esta
import emission.core.wrapper.entry as ecwe

def get_last_entry(user_id, time_query, config_key):
    user_ts = esta.TimeSeries.get_time_series(user_id)
    
    # get the max write_ts for this stream, which corresponds to the last entry
    # We expect this to be small, unless users are continuously overriding values
    config_overrides = list(user_ts.find_entries([config_key], time_query))
    logging.debug("Found %d user overrides for user %s" % (len(config_overrides), user_id))
    if len(config_overrides) == 0:
        logging.warning("No user defined overrides for %s, early return" % user_id)
        return None
    else:
        # entries are sorted by the write_ts, we can take the last value
        return ecwe.Entry(config_overrides[-1]).data
