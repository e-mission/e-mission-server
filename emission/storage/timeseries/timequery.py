class TimeQuery(object):
    """
        Object that encapsulates a query for a particular time (read_ts, write_ts, or processed_ts)
    """
    def __init__(self, timeType, startTs, endTs):
        self.timeType = timeType
        self.startTs = startTs
        self.endTs = endTs

    def get_query(self):
        time_key = self.timeType
        ret_query = {time_key : {"$lte": self.endTs}}
        if (self.startTs is not None):
            ret_query[time_key].update({"$gte": self.startTs})
        return ret_query

