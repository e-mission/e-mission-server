
from abc import ABC, abstractmethod

#make abstract
class FakeUser:
    def __init__(self, email, config={}):
        self._email = email
        self._config = config
        self._current_state = config['initial_state']

