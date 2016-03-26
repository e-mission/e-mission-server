import logging
import emission.storage.timeseries.abstract_timeseries as esta

def get_last_entry(user_id, config_key):
    user_ts = esta.TimeSeries.get_time_series(user_id)
    
    # get the max write_ts for this stream, which corresponds to the last entry
    max_config_ts = ts.get_max_value_for_field(config_key, "metadata.write_ts")
    logging.debug("last written user config = %s" % max_config_ts)
    if max_config_ts == -1:
        logging.warning("No user defined config for %s, early return" % )
        return None
    else:
        last_config = ts.get_entry_at_ts(config_key, "metadata.write_ts", max_config_ts)
        return last_config
