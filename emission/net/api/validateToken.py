import urllib, json
from jwcrypto import jwt, jwk


class OpenIDTokenValidator:
    def __init__(self, config_url, audience):
        """
        Retrieve auth server config and set up the validator

        :param config_url: the discovery URI
        :param audience: client ID to verify against
        """
        # Fetch configuration
        self.config = json.loads(OpenIDTokenValidator.__fetch_content__(config_url))
        self.config['audience'] = audience

        # Fetch signing key/certificate
        jwk_response = OpenIDTokenValidator.__fetch_content__(self.config['jwks_uri'])
        self.jwk_keyset = jwk.JWKSet.from_json(jwk_response)

    @staticmethod
    def __fetch_content__(url):
        response = urllib.urlopen(url)
        return response.read()

    def __verify_claim__(self, decoded_token_json):
        if decoded_token_json['iss'] != self.config['issuer']:
            raise Exception('Invalid Issuer')
        if decoded_token_json['aud'] != self.config['audience']:
            raise Exception('Invalid Audience')

    def verify_and_decode_token(self, token):
        """
        Verify the token with the provided JWK certificate and claims

        :param token: the token to verify
        :return: the decoded ID token body
        """
        decoded_token = jwt.JWT(key=self.jwk_keyset, jwt=token)
        decoded_json = json.loads(decoded_token.claims)
        self.__verify_claim__(decoded_json)
        return decoded_json

