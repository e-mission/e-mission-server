import logging
import json
import traceback
import requests

# For decoding JWTs on the client side
import oauth2client.client
from oauth2client.crypt import AppIdentityError

class GoogleAuthMethod:
    def __init__(self):
        key_file = open('conf/net/auth/google_auth.json')
        key_data = json.load(key_file)
        self.client_key = key_data["client_key"]
        self.client_key_old = key_data["client_key_old"]
        self.ios_client_key = key_data["ios_client_key"]
        self.ios_client_key_new = key_data["ios_client_key_new"]

    def verifyUserToken(self, token):
        try:
            # attempt to validate token on the client-side
            logging.debug("Using OAuth2Client to verify id token of length %d from android phones" % len(token))
            tokenFields = oauth2client.client.verify_id_token(token,self.client_key)
            logging.debug(tokenFields)
        except AppIdentityError as androidExp:
            try:
                logging.debug("Using OAuth2Client to verify id token of length %d from android phones using old token" % len(token))
                tokenFields = oauth2client.client.verify_id_token(token,self.client_key_old)
                logging.debug(tokenFields)
            except AppIdentityError as androidExpOld:
                try:
                    logging.debug("Using OAuth2Client to verify id token from iOS phones")
                    tokenFields = oauth2client.client.verify_id_token(token, self.ios_client_key)
                    logging.debug(tokenFields)
                except AppIdentityError as iOSExp:
                    try:
                        logging.debug("Using OAuth2Client to verify id token from newer iOS phones")
                        tokenFields = oauth2client.client.verify_id_token(token, self.ios_client_key_new)
                        logging.debug(tokenFields)
                    except AppIdentityError as iOSExp:
                        traceback.print_exc()
                        logging.debug("OAuth failed to verify id token, falling back to constructedURL")
                        #fallback to verifying using Google API
                        constructedURL = ("https://www.googleapis.com/oauth2/v1/tokeninfo?id_token=%s" % token)
                        r = requests.get(constructedURL)
                        tokenFields = json.loads(r.content)
                        logging.debug("tokenFields = %s" % tokenFields)
                        if 'audience' not in tokenFields:
                            raise ValueError("Invalid token %s" % tokenFields)
                        in_client_key = tokenFields['audience']
                        if (in_client_key != self.client_key and
                            in_client_key != self.client_key_old):
                            if (in_client_key != self.ios_client_key and 
                                in_client_key != self.ios_client_key_new):
                                raise ValueError("Invalid client key %s" % in_client_key)
        logging.debug("Found user email %s" % tokenFields['email'])
        return tokenFields['email']

