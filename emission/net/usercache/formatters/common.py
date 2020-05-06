from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import pytz
import logging
import arrow

import emission.core.wrapper.localdate as ecwl

def expand_metadata_times(m):
    logging.debug("write_ts = %s" % m.write_ts)
    # Write DT will also be in local time to allow us to search for all trips on a particular
    # day or all movement in a particular location between a particular time range without doing
    # string parsing. If we didn't do this, there is no way to know when a
    # particular event occurred in local time without string parsing.
    # The timestamp is in UTC and doesn't know the local time.
    # The fmt_time is in local time, but is a string, so query ranges are hard
    # to search for
    m.write_local_dt = ecwl.LocalDate.get_local_date(m.write_ts, m.time_zone)
    m.write_fmt_time = arrow.get(m.write_ts).to(m.time_zone).isoformat()

def expand_data_times(d,m):
    d.local_dt = ecwl.LocalDate.get_local_date(d.ts, m.time_zone)
    d.fmt_time = arrow.get(d.ts).to(m.time_zone).isoformat()

def expand_start_end_data_times(d,m):
    d.start_local_dt = ecwl.LocalDate.get_local_date(d.start_ts, m.time_zone)
    d.start_fmt_time = arrow.get(d.start_ts).to(m.time_zone).isoformat()
    d.end_local_dt = ecwl.LocalDate.get_local_date(d.end_ts, m.time_zone)
    d.end_fmt_time = arrow.get(d.end_ts).to(m.time_zone).isoformat()
