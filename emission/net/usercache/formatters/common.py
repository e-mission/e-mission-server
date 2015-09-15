import datetime as pydt
import pytz
import logging

def expand_metadata_times(m):
    logging.debug("write_ts = %s" % m.write_ts)
    m.write_dt = pydt.datetime.utcfromtimestamp(m.write_ts).replace(tzinfo=pytz.utc)
    m.write_fmt_time = str(m.write_dt.astimezone(pytz.timezone(m.time_zone)))
