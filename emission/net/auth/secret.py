import logging
import json
import traceback

class SecretMethod(object):
    def __init__(self):
        key_file = open('conf/net/auth/secret_list.json')
        key_data = json.load(key_file)
        key_file.close()
        self.client_secret_list = key_data["client_secret_list"]

    def verifyUserToken(self, token):
        # attempt to validate token on the client-side
        logging.debug("Using the SecretAuthMethod to verify id token %s of length %d against secret list %s..." % 
            (token, len(token), self.client_secret_list[0:10]))
        # matching_list = [token == curr_token for curr_token in self.token_list]
        # print matching_list
        # stripped_matching_list = [token == curr_token.strip() for curr_token in self.token_list]
        # print stripped_matching_list
        for secret in self.client_secret_list:
            if token.startswith(secret):
                logging.debug("Found match for secret %s of length %d" % (secret, len(secret)))
                # In this case, the token is the email, since we don't actually
                # have the user email
                return token
        # If we get here, we have not returned, so the token did not start with
        # a valid secret
        raise ValueError("Invalid token %s, not found in list of length %d" % 
            (token, len(self.client_secret_list)))
