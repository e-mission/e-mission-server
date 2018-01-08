from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from builtins import object
import logging
import json
import traceback
import requests

class SkipMethod(object):
    def __init__(self):
        pass

    def verifyUserToken(self, token):
        logging.debug("Using the skip method to verify id token %s of length %d" %
            (token, len(token)))
        return token # When we skip, the token is the user's email directly
