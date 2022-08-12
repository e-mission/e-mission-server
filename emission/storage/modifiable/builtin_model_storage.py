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
        logging.debug("Inserting entry %s into model DB" % entry)
        ins_result = edb.get_model_db().insert_one(entry)
        ## TODO: Cleanup old/obsolete models
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

