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

class TokenListMethod(object):
    def __init__(self):
        key_file = open('conf/net/auth/token_list.json')
        key_data = json.load(key_file)
        key_file.close()
        self.token_list_file = key_data["token_list"]
        raw_token_list = open(self.token_list_file).readlines()
        self.token_list = [t.strip() for t in raw_token_list]
        raw_token_list = None

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
