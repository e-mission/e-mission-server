import logging

import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta

class BuiltinTimeSeries(esta.TimeSeries):
    def __init__(self, uuid):
        super(BuiltinTimeSeries, self).__init__(uuid)
        self.key_query = lambda(key): {"metadata.key": key}
        self.ts_query = lambda(tq): {"$and": [{"metadata.%s" % tq.timeType: {"$gte": tq.startTs}},
                {"metadata.%s" % tq.timeType: {"$lte": tq.endTs}}]}
        self.type_query = lambda(entry_type): {"metadata.type": entry_type}

    def _get_query(self, key_list = None, time_query = None):
        ret_query = {'user_id': self.uuid} # UUID is mandatory
        if key_list is not None and len(key_list) > 0:
            key_query_list = []
            for key in key_list:
                key_query_list.append(self.key_query(key))
            ret_query.update({"$or": key_query_list})
        if time_query is not None:
            ret_query.update(self.ts_query(time_query))
        return ret_query

    def find_entries(self, key_list = None, time_query = None):
        return edb.get_timeseries_db().find(self._get_query(key_list, time_query))

    def insert(self, entry):
        """
        """
        logging.debug("insert called")
        if "user_id" not in entry:
            entry["user_id"] = self.uuid
        elif entry["user_id"] != self.uuid:
            raise AttributeError("Saving entry for %s in timeseries for %s" % (entry["user_id"], self.uuid))
        else:
            logging.debug("entry was fine, no need to fix it")

        logging.debug("Inserting entry %s into timeseries" % entry)
        edb.get_timeseries_db().insert(entry)

    def insert_error(self, entry):
        """
        """
        logging.debug("insert_error called")
        if "user_id" not in entry:
            entry["user_id"] = self.uuid
        elif entry["user_id"] != self.uuid:
            raise AttributeError("Saving entry for %s in timeseries for %s" % (entry["user_id"], self.uuid))
        else:
            logging.debug("entry was fine, no need to fix it")

        logging.debug("Inserting entry %s into error timeseries" % entry)
        edb.get_timeseries_error_db().insert(entry)
