from enum import Enum
from typing import Dict
import emission.analysis.modelling.user_label_model.util as util

class ModelStorage(Enum):
    FILE_SYSTEM = 0
    DATABASE = 1
    @classmethod
    def names(cls):
        return list(map(lambda e: e.name, list(cls)))


def create_filename(user_id, model_name) -> str:
    return f"user_label_model_{model_name}_{str(user_id)}"


def create_database_table_name(model_name) -> str:
    return f"user_label_model_{model_name}"


def load(user_id, model_name: str, model_storage: ModelStorage) -> Dict:
    """load a user label model from a model storage location

    :param user_id: the user to request a model for
    :type user_id: UUID
    :param model_name: _description_
    :type model_name: str
    :param save_format: _description_
    :type save_format: SaveFormat
    :return: _description_
    :rtype: Dict
    """
    if model_storage == ModelStorage.FILE_SYSTEM:
        filename = create_filename(user_id, model_name)
        model_data = util.load_fs(filename)
        return model_data
    elif model_storage == ModelStorage.DATABASE:
        table_name = create_database_table_name(model_name)
        model_data = util.load_db(user_id, table_name)
        return model_data
    else:
        storage_types_str = ",".join(ModelStorage.names())
        msg = (
            f"unknown model storage type {model_storage}, must be one of "
            f"{{{storage_types_str}}}"
        )
        raise TypeError(msg)

def save(user_id, model_data: Dict, model_name: str, model_storage: ModelStorage):
    try:
        if model_storage == ModelStorage.FILE_SYSTEM:
            filename = create_filename(user_id, model_name)
            util.save_fs(filename, model_data)
        elif model_storage == ModelStorage.DATABASE:
            table_name = create_database_table_name(model_name)
            util.save_db(user_id, table_name, model_data)
        else:
            storage_types_str = ",".join(ModelStorage.names())
            msg = (
                f"unknown model storage type {model_storage}, must be one of "
                f"{{{storage_types_str}}}"
            )
            raise TypeError(msg)
    except IOError as e:
        msg = (
            f"cannot save model for user {user_id}, model_name {model_name} "
            f"to the file system"
        )
        raise IOError(msg) from e
