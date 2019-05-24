from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from builtins import object
import urllib.request, urllib.parse, urllib.error, json
import json
import logging
from jwcrypto import jwt, jwk


class OpenIDAuthMethod(object):
    def __init__(self):
        """
        Retrieve auth server config and set up the validator

        :param config_url: the discovery URI
        :param audience: client ID to verify against
        """
        # first read the parameters from the keys.json file
        # including the config_url and the audience
        key_file = open('conf/net/auth/openid_auth.json')
        key_data = json.load(key_file)
        discoveryURI = key_data["discoveryURI"]
        audience = key_data["clientID"]
        # Now, use them to retrieve the configuration
        self.config = json.loads(OpenIDAuthMethod.__fetch_content__(discoveryURI))
        self.config['audience'] = audience

        # Fetch signing key/certificate
        jwk_response = OpenIDAuthMethod.__fetch_content__(self.config['jwks_uri'])
        self.jwk_keyset = jwk.JWKSet.from_json(jwk_response)

    @staticmethod
    def __fetch_content__(url):
        response = urllib.request.urlopen(url)
        return response.read()

    def __verify_claim__(self, decoded_token_json):
        if decoded_token_json['iss'] != self.config['issuer']:
            raise Exception('Invalid Issuer')
        if decoded_token_json['aud'] != self.config['audience']:
            raise Exception('Invalid Audience')

    def verifyUserToken(self, token):
        """
        Verify the token with the provided JWK certificate and claims

        :param token: the token to verify
        :return: the decoded ID token body
        """
        decoded_token = jwt.JWT(key=self.jwk_keyset, jwt=token)
        decoded_json = json.loads(decoded_token.claims)
        logging.debug("decoded_json = %s" % decoded_json)
        self.__verify_claim__(decoded_json)
        email = decoded_json['email']
        logging.debug("After verifying claim, returning valid email = %s" % email)
        return email
