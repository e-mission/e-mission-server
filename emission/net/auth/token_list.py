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
import emission.storage.decorations.token_queries as esdt

class TokenListMethod(object):
    def __init__(self, source='conf/net/auth/token_list.json', userid=0):
        self.token_list = esdt.get_all_tokens()

    def verifyUserToken(self, token):
        # attempt to validate token on the client-side
        logging.debug("Using the TokenListMethod to verify id token %s of length %d against list %s..." % 
            (token, len(token), self.token_list[0:10]))
        # matching_list = [token == curr_token for curr_token in self.token_list]
        # print matching_list
        # stripped_matching_list = [token == curr_token.strip() for curr_token in self.token_list]
        # print stripped_matching_list
        if token in self.token_list:
            logging.debug("Found match for token %s of length %d" % (token, len(token)))
            # In this case, the token is the email, since we don't actually
            # have the user email
            return token
        else:
            raise ValueError("Invalid token %s, not found in list of length %d" % 
                (token, len(self.token_list)))
