import logging
from typing import Optional

from pymongo import ReturnDocument

import emission.core.get_database as edb
import emission.core.wrapper.metadata as ecwm
import emission.core.wrapper.wrapperbase as ecwb
import emission.storage.modifiable.abstract_state_storage as esas


class BuiltinStateStorage(esas.StateStorage):
    def __init__(self, user_id):
        super(BuiltinStateStorage, self).__init__(user_id)

    def _get_state_wrapper_class(self, state_name: esas.StateName):
        wrapper_name = esas.StateStorage.get_state_wrapper(state_name)
        return ecwb.WrapperBase._get_class(wrapper_name)

    def _state_key(self, state_name: esas.StateName) -> str:
        return "state/%s" % state_name.value.lower()

    def upsert_state(self, state_name: esas.StateName, state: ecwb.WrapperBase):
        key = self._state_key(state_name)
        state_wrapper_class = self._get_state_wrapper_class(state_name)
        if isinstance(state, state_wrapper_class):
            state_to_store = state
        else:
            raise TypeError("state must be a wrapper instance of the mapped state wrapper class")

        entry = {
            "user_id": self.user_id,
            "metadata": ecwm.Metadata.create_metadata_for_result(key),
            "data": state_to_store,
        }
        find_query = {"user_id": self.user_id, "metadata.key": key}
        logging.debug("Upserting entry %s into state DB" % entry)
        updated_doc = edb.get_state_db().find_one_and_replace(
            find_query,
            entry,
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        assert edb.get_state_db().count_documents(find_query) == 1, (
            "There should be exactly one entry for %s after upsert" % key
        )
        return updated_doc["_id"]

    def get_current_state(self, state_name: esas.StateName) -> Optional[ecwb.WrapperBase]:
        key = self._state_key(state_name)
        state_wrapper_class = self._get_state_wrapper_class(state_name)
        find_query = {"user_id": self.user_id, "metadata.key": key}
        result_it = edb.get_state_db().find(find_query).sort("metadata.write_ts", -1).limit(1)
        result_list = list(result_it)
        assert len(result_list) <= 1, "There should be at most one entry for %s" % key
        if len(result_list) == 0:
            return None
        first_entry = result_list[0]
        return state_wrapper_class(first_entry["data"])

    def delete_state(self, state_name: esas.StateName):
        key = self._state_key(state_name)
        find_query = {"user_id": self.user_id, "metadata.key": key}
        assert edb.get_state_db().count_documents(find_query) <= 1, (
            "There should be at most one entry for %s" % key
        )
        deleted = edb.get_state_db().delete_many(find_query)
        logging.debug("Deleted %s existing state entries for %s" % (deleted.deleted_count, key))
