from builtins import object

class FmtTimeQuery(object):
    """
    Object that encapsulates a query for an inclusive range between two ISO-format strings.
    Useful for querying based on the local date/time at which data was collected,
      like with timeType of "data.fmt_time" or "data.start_fmt_time".
    e.g. FmtTimeQuery("data.fmt_time", "2024-01", "2024-03") # first quarter of 2024
    e.g. FmtTimeQuery("data.fmt_time", "2024-05-01", "2024-05-31") # all of May 2024
    e.g. FmtTimeQuery("data.fmt_time", "2024-06-03T08:00", "2024-06-03T16:59") # work hours on Jun 3 2024
    """
    def __init__(self, timeType: str, startIso: str, endIso: str) -> None:
        self.timeType = timeType
        self.startIso = startIso
        # append 'Z' to make the end range inclusive
        # (because Z is greater than any other character that can appear in an ISO string)
        self.endIso = endIso + 'Z'

    def get_query(self) -> dict:
        time_key = self.timeType
        ret_query = {time_key: {"$lte": self.endIso}}
        if (self.startIso is not None):
            ret_query[time_key].update({"$gte": self.startIso})
        return ret_query

    def __repr__(self) -> str:
        return f"FmtTimeQuery {self.timeType} with range [{self.startIso}, {self.endIso})"
