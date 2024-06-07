from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from builtins import object
class TimeQuery(object):
    """
    Object that encapsulates a query for a range of time [start_time, end_time]
    Can query by Unix timestamps with a '*_ts' time_type (like "metadata.write_ts", "data.ts", or "data.start_ts")
      e.g. TimeQuery("metadata.write_ts", 1234567890, 1234567900)
    Or, can query by ISO datetime strings with a '*_fmt_time' time_type (like "data.fmt_time" or "data.start_fmt_time")
      This is useful for querying based on the local date/time at which data was collected
      e.g. TimeQuery("data.fmt_time", "2024-06-03T08:00", "2024-06-03T16:59")
    """
    def __init__(self, time_type, start_time, end_time):
        self.time_type = time_type
        self.start_time = start_time
        # if end_time is an ISO string, append 'Z' to make the end range inclusive
        # (because Z is greater than any other character that can appear in an ISO string)
        self.end_time = end_time + 'Z' if isinstance(end_time, str) else end_time

    def get_query(self):
        time_key = self.time_type
        ret_query = {time_key : {"$lte": self.end_time}}
        if (self.start_time is not None):
            ret_query[time_key].update({"$gte": self.start_time})
        return ret_query

    def __repr__(self):
        return f"TimeQuery {self.time_type} with range [{self.start_time}, {self.end_time})"
