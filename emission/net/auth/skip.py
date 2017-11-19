import logging
import json
import traceback
import requests

class SkipMethod:
    def __init__(self):
        pass

    def verifyUserToken(self, token):
        logging.debug("Using the skip method to verify id token %s of length %d" %
            (token, len(token)))
        return token # When we skip, the token is the user's email directly
