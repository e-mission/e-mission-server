import logging
import enum as enum
from typing import Dict, Optional
import emission.core.wrapper.wrapperbase as ecwb

class ModelStorage(object):
    @staticmethod
    def get_model_storage(user_id):
        """
        :param user_id: the user_id that we want the timeseries for
        :returns: a model storage for that particular user
        """
        import emission.storage.modifiable.builtin_model_storage as bims
        return bims.BuiltinModelStorage(user_id)

    def __init__(self, user_id):
        self.user_id = user_id

    def upsert_model(self, key: str, model: ecwb.WrapperBase):
        """
        :param: the metadata key for the entries, used to identify the model type
        :model: a wrapper for the model
        """
        pass

    def get_current_model(self, key:str) -> Optional[Dict]:
        """
        : param key: the metadata key for the entries, used to identify the model type
        : return: the most recent database entry for this key
        """
        pass
