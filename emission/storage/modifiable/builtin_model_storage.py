import logging
import pandas as pd
import pymongo
import itertools
from typing import Dict, Optional

import emission.core.get_database as edb
import emission.storage.modifiable.abstract_model_storage as esma
import emission.analysis.modelling.trip_model.config as eamtc
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.wrapperbase as ecwb

class BuiltinModelStorage(esma.ModelStorage):
    def __init__(self, user_id):
        super(BuiltinModelStorage, self).__init__(user_id)
        self.key_query = lambda key: {"metadata.key": key}
        self.user_query = {"user_id": self.user_id} # UUID is mandatory for this version

    def upsert_model(self, key:str, model: ecwb.WrapperBase):
        """
        :param: the metadata key for the entries, used to identify the model type
        :model: a wrapper for the model
        """
        logging.debug("upsert_doc called with key %s" % key)
        entry = ecwe.Entry.create_entry(self.user_id, key, model)
        # Cleaning up older models, before inserting new model
        self.trim_model_entries(key)
        logging.debug("Inserting entry %s into model DB" % entry)
        ins_result = edb.get_model_db().insert_one(entry)
        new_model_count = edb.get_model_db().count_documents({"user_id": self.user_id})
        logging.debug("New model count for user %s = %s" % (self.user_id, new_model_count))
        return ins_result.inserted_id

    def get_current_model(self, key:str) -> Optional[Dict]:
        """
        :param key: the metadata key for the entries, used to identify the model type
        :return: the most recent database entry for this key
        """
        find_query = {"user_id": self.user_id, "metadata.key": key}
        result_it = edb.get_model_db().find(find_query).sort("metadata.write_ts", -1).limit(1)
        # this differs from the timeseries `get_first_entry` only in the find query
        # and the fact that the sort key and sort order are hardcoded
        # everything below this point is identical
        # but it is also fairly trivial, so I am not sure it is worth pulling
        # out into common code at this point
        result_list = list(result_it)
        if len(result_list) == 0:
            return None
        else:
            first_entry = result_list[0]
            del first_entry["_id"]
            return first_entry 

    def trim_model_entries(self, key:str):
        """
        :param: the metadata key for the entries, used to identify the model type
        This function is called inside the model insertion function just before the 
        model is inserted, to ensure older models are removed before inserting newer ones.

        The flow of model insertion function calls is:
        eamur.update_trip_model() -> eamums.save_model() -> esma.upsert_model() -> esma.trim_model_entries()
        """
        old_model_count = edb.get_model_db().count_documents({"user_id": self.user_id})
        deleted_model_count = 0
        find_query = {"user_id": self.user_id, "metadata.key": key}
        result_it = edb.get_model_db().find(find_query).sort("metadata.write_ts", -1)
        result_list = list(result_it)
        maximum_stored_model_count = eamtc.get_maximum_stored_model_count()
        if old_model_count >= maximum_stored_model_count:
            # Specify the last or minimum timestamp of Kth model entry
            write_ts_limit = result_list[maximum_stored_model_count - 1]['metadata']['write_ts']
            logging.debug(f"Write ts limit = {write_ts_limit}")
            filter_clause = {
                "user_id" : self.user_id,
                "metadata.key" : key,
                "metadata.write_ts" : { "$lte" : write_ts_limit }
            }
            models_to_delete = edb.get_model_db().delete_many(filter_clause)
            deleted_model_count = models_to_delete.deleted_count
        new_model_count = edb.get_model_db().count_documents({"user_id": self.user_id})
        if deleted_model_count > 0:
            logging.debug(f"{deleted_model_count} models deleted successfully")
            logging.debug("Model count for user %s has changed %s -> %s" % (self.user_id, old_model_count, new_model_count))
        else:
            logging.debug("No models found or none deleted")
            logging.debug("Model count for user %s unchanged %s -> %s" % (self.user_id, old_model_count, new_model_count))