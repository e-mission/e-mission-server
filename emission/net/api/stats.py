import logging
import emission.storage.decorations.stats_queries as esds

def store_server_api_time(user_id, call, ts, reading):
    esds.store_server_api_time(user_id, call, ts, reading)

def store_server_api_error(user_id, call, ts, reading):
    esds.store_server_api_error(user_id, call, ts, reading)

# Backward compat to store old-style client stats until the phone upgrade
# has been pushed out to all phones
def setClientMeasurements(user_id, reportedVals):
    logging.info("Received %d client keys and %d client readings for user_id %s" % (len(reportedVals['Readings']),
                                                                                 getClientMeasurementCount(reportedVals['Readings']), user_id))
    logging.debug("reportedVals = %s" % reportedVals)
    metadata = reportedVals['Metadata']
    stats = reportedVals['Readings']
    for key in stats:
        values = stats[key]
        for value in values:
            storeClientEntry(user_id, key, value[0], value[1], metadata)

def getClientMeasurementCount(readings):
    retSum = 0
    for currReading in readings:
        currArray = readings[currReading]
        # logging.debug("currArray for reading %s is %s and its length is %d" % (currReading, currArray, len(currArray)))
        retSum += len(currArray)
    return retSum

def storeClientEntry(user_id, key, ts, reading, metadata):
    logging.debug("storing client entry for user_id %s, key %s at timestamp %s" % (user_id, key, ts))

    old_style_data = createEntry(user_id, key, ts, reading)
    old_style_data.update(metadata)
    save_to_timeseries(old_style_data)

def save_to_timeseries(old_style_data):
    import emission.storage.timeseries.abstract_timeseries as esta

    user_id = old_style_data["user"]
    new_entry = old2new(old_style_data)
    return esta.TimeSeries.get_time_series(user_id).insert(new_entry)

def old2new(old_style_data):
    import emission.core.wrapper.entry as ecwe
    import emission.core.wrapper.statsevent as ecws
    import emission.core.wrapper.battery as ecwb

    int_with_none = lambda s: float(s) if s is not None else None

    user_id = old_style_data["user"]
    del old_style_data["user"]
    if old_style_data["stat"] == "battery_level":
        new_style_data = ecwb.Battery({
            "battery_level_pct" : int_with_none(old_style_data["reading"]),
            "ts": old_style_data["ts"]
        })
        new_key = "background/battery"
    else:
        new_style_data = ecws.Statsevent()
        new_style_data.name = old_style_data["stat"]
        new_style_data.ts = old_style_data["ts"]
        new_style_data.reading = int_with_none(old_style_data["reading"])
        new_style_data.client_app_version = old_style_data["client_app_version"]
        new_style_data.client_os_version = old_style_data["client_os_version"]
        new_key = stat2key(old_style_data["stat"])

    new_entry = ecwe.Entry.create_entry(user_id, new_key, new_style_data)
    # For legacy entries, make sure that the write_ts doesn't become the conversion
    # time or the server arrival time
    new_entry["metadata"]["write_ts"] = new_style_data.ts
    del new_entry["metadata"]["write_local_dt"]
    del new_entry["metadata"]["write_fmt_time"]
    # We are not going to fill in the local_date and fmt_time entries because
    # a) we don't know what they are for legacy entries
    # b) we don't even know whether we need to use them
    # c) even if we do, we don't know if we need to use them for older entries
    # So let's leave the hacky reconstruction algorithm until we know that we really need it
    return new_entry

def stat2key(stat_name):
    stat_name_mapping = {
        "app_launched": "stats/client_nav_event",
        "push_stats_duration": "stats/client_time",
        "sync_duration": "stats/client_time",
        "sync_launched": "stats/client_nav_event",
        "button_sync_forced": "stats/client_nav_event",
        "sync_pull_list_size": "stats/client_time",
        "pull_duration": "stats/client_time"
    }
    return stat_name_mapping[stat_name]


def createEntry(user, stat, ts, reading):
   return {'user': user,
           'stat': stat,
           'ts': float(ts),
           'reading': reading}

