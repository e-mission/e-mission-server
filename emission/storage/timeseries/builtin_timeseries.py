import logging
import pandas as pd
import pymongo

import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta

class BuiltinTimeSeries(esta.TimeSeries):
    def __init__(self, user_id):
        super(BuiltinTimeSeries, self).__init__(user_id)
        self.key_query = lambda(key): {"metadata.key": key}
        self.type_query = lambda(entry_type): {"metadata.type": entry_type}

    @staticmethod
    def get_uuid_list():
        return edb.get_timeseries_db().distinct("user_id")

    @staticmethod
    def ts_query(tq):
        time_key = "metadata.%s" % tq.timeType
        ret_query = {time_key : {"$lt": tq.endTs}}
        if (tq.startTs is not None):
            ret_query[time_key].update({"$gte": tq.startTs})
        return ret_query

    def _get_query(self, key_list = None, time_query = None):
        ret_query = {'user_id': self.user_id} # UUID is mandatory
        if key_list is not None and len(key_list) > 0:
            key_query_list = []
            for key in key_list:
                key_query_list.append(self.key_query(key))
            ret_query.update({"$or": key_query_list})
        if time_query is not None:
            ret_query.update(self.ts_query(time_query))
        return ret_query

    @staticmethod
    def _get_sort_key(time_query = None):
        if time_query is None:
            return "metadata.write_ts"
        else:
            return "metadata.%s" % time_query.timeType

    @staticmethod
    def _to_df_entry(entry):
        ret_val = entry["data"]
        ret_val["_id"] = entry["_id"]
        # logging.debug("ret_val = %s " % ret_val)
        return ret_val

    def find_entries(self, key_list = None, time_query = None):
        sort_key = self._get_sort_key(time_query)
        logging.debug("sort_key = %s" % sort_key)
        return edb.get_timeseries_db().find(self._get_query(key_list, time_query)).sort(sort_key, pymongo.ASCENDING)

    def get_entry_at_ts(self, key, ts_key, ts):
        return edb.get_timeseries_db().find_one({"user_id": self.user_id,
                                                 "metadata.key": key,
                                                 ts_key: ts})

    def get_data_df(self, key, time_query = None):

        sort_key = self._get_sort_key(time_query)
        logging.debug("curr_query = %s, sort_key = %s" % (self._get_query([key], time_query), sort_key))
        result_it = edb.get_timeseries_db().find(self._get_query([key], time_query), {"data": True})\
                                            .sort(sort_key, pymongo.ASCENDING)
        # Dataframe doesn't like to work off an iterator - it wants everything in memory
        return pd.DataFrame([BuiltinTimeSeries._to_df_entry(e) for e in list(result_it)])

    def insert(self, entry):
        """
        """
        logging.debug("insert called")
        if "user_id" not in entry:
            entry["user_id"] = self.user_id
        elif entry["user_id"] != self.user_id:
            raise AttributeError("Saving entry for %s in timeseries for %s" % (entry["user_id"], self.user_id))
        else:
            logging.debug("entry was fine, no need to fix it")

        logging.debug("Inserting entry %s into timeseries" % entry)
        edb.get_timeseries_db().insert(entry)

    def insert_error(self, entry):
        """
        """
        logging.debug("insert_error called")
        if "user_id" not in entry:
            entry["user_id"] = self.user_id
        elif entry["user_id"] != self.user_id:
            raise AttributeError("Saving entry for %s in timeseries for %s" % (entry["user_id"], self.user_id))
        else:
            logging.debug("entry was fine, no need to fix it")

        logging.debug("Inserting entry %s into error timeseries" % entry)
        edb.get_timeseries_error_db().insert(entry)
