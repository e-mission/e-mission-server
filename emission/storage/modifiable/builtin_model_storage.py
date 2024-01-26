import logging
import pandas as pd
import pymongo
import itertools
from typing import Dict, Optional

import emission.core.get_database as edb
import emission.storage.modifiable.abstract_model_storage as esma

import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.wrapperbase as ecwb

class BuiltinModelStorage(esma.ModelStorage):
    # TODO: Discuss how to decide on model_count limit
    K_MODEL_COUNT = 10

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
        ## TODO: Cleanup old/obsolete models
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

        current_model_count = edb.get_model_db().count_documents({"user_id": self.user_id})
        logging.debug("Before trimming, model count for user %s = %s" % (self.user_id, current_model_count))
        find_query = {"user_id": self.user_id, "metadata.key": key}
        result_it = edb.get_model_db().find(find_query).sort("metadata.write_ts", -1)
        result_list = list(result_it)

        if current_model_count >= self.K_MODEL_COUNT:
            # Specify the last or minimum timestamp of Kth model entry
            write_ts_limit = result_list[self.K_MODEL_COUNT - 1]['metadata']['write_ts']
            logging.debug(f"Write ts limit = {write_ts_limit}")

            filter_clause = {
                "user_id" : self.user_id,
                "metadata.key" : key,
                "metadata.write_ts" : { "$lte" : write_ts_limit }
            }

            models_to_delete = edb.get_model_db().delete_many(filter_clause)

            if models_to_delete.deleted_count > 0:
                logging.debug(f"{models_to_delete.deleted_count} documents deleted successfully\n")
            else:
                logging.debug("No documents found or none deleted\n")

        new_model_count = edb.get_model_db().count_documents({"user_id": self.user_id})
        logging.debug("After trimming, model count for user %s = %s" % (self.user_id, new_model_count))