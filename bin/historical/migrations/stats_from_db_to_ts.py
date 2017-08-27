import logging

import emission.core.get_database as edb
import emission.net.api.stats as enas

def save_to_timeseries_server(old_style_data):
    import emission.storage.timeseries.abstract_timeseries as esta

    user_id = old_style_data["user"]
    new_entry = old2new_server(old_style_data)
    return esta.TimeSeries.get_time_series(user_id).insert(new_entry)

def old2new_server(old_style_data):
    import emission.core.wrapper.entry as ecwe
    import emission.core.wrapper.statsevent as ecws
    import emission.core.wrapper.battery as ecwb

    none2None = lambda s: None if s == 'none' else s
    float_with_none = lambda s: float(s) if s is not None else None

    user_id = old_style_data["user"]
    del old_style_data["user"]

    new_style_data = ecws.Statsevent()
    new_style_data.name = old_style_data["stat"].replace(" ", "_")
    if "client_ts" in old_style_data:
        new_style_data.ts = old_style_data["client_ts"]
    else:
        new_style_data.ts = old_style_data["ts"]
    new_style_data.reading = float_with_none(none2None(old_style_data["reading"]))
    new_key = "stats/server_api_time"

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

def convertClientStats(collection):
    for old_entry in collection.find():
        try:
            enas.save_to_timeseries(old_entry)
        except:
            logging.error("Error converting entry %s" % old_entry)
            raise

def convertServerStats(collection):
    for old_entry in collection.find():
        try:
            save_to_timeseries_server(old_entry)
        except:
            logging.error("Error converting entry %s" % old_entry)
            raise

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    # No arguments - muahahahaha. Just going to convert everything.
    logging.info("About to convert client stats")
    convertClientStats(edb.get_client_stats_db_backup())

    logging.info("About to convert server stats")
    convertServerStats(edb.get_server_stats_db_backup())

    logging.info("Not about to convert result stats - they are no longer relevant")
