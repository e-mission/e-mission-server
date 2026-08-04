import enum
from typing import Optional, Union

import emission.core.wrapper.wrapperbase as ecwb


class StateName(enum.Enum):
    VEHICLE = "VEHICLE"
    PAYMENT = "PAYMENT"


class StateStorage(object):
    @staticmethod
    def _getStateName2Wrapper():
        return {
            StateName.VEHICLE: "vehicle",
            StateName.PAYMENT: "payment",
        }

    @staticmethod
    def get_state_wrapper(state_name: Union[StateName, str]) -> str:
        if isinstance(state_name, str):
            state_name = StateName[state_name]
        return StateStorage._getStateName2Wrapper()[state_name]

    @staticmethod
    def get_state_storage(user_id):
        """
        :param user_id: the user_id that we want state storage for
        :returns: a state storage for that particular user
        """
        import emission.storage.modifiable.builtin_state_storage as bsts

        return bsts.BuiltinStateStorage(user_id)

    def __init__(self, user_id):
        self.user_id = user_id

    def upsert_state(self, state_name: StateName, state: ecwb.WrapperBase):
        """
        :param state_name: enum-backed state name
        :param state: payload to persist
        """
        pass

    def get_current_state(self, state_name: StateName) -> Optional[ecwb.WrapperBase]:
        """
        :param state_name: enum-backed state name
        :returns: the most recent state for this state_name
        """
        pass

    def delete_state(self, state_name: StateName):
        """
        :param state_name: enum-backed state name
        """
        pass
