import datetime as pydt
import pytz
import logging

def expand_metadata_times(m):
    logging.debug("write_ts = %s" % m.write_ts)
    # Write DT will also be in local time to allow us to search for all trips on a particular
    # day or all movement in a particular location between a particular time range without doing
    # string parsing. If we didn't do this, there is no way to know when a
    # particular event occurred in local time without string parsing.
    # The timestamp is in UTC and doesn't know the local time.
    # The fmt_time is in local time, but is a string, so query ranges are hard
    # to search for
    local_aware_dt = pydt.datetime.utcfromtimestamp(m.write_ts).replace(tzinfo=pytz.utc).astimezone(pytz.timezone(m.time_zone))
    m.write_local_dt = local_aware_dt.replace(tzinfo=None)
    m.write_fmt_time = local_aware_dt.isoformat()
