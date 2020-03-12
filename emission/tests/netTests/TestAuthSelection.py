from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import json
import sys
import os
import uuid
import logging
import time

# Our imports
import emission.tests.common as etc
import emission.net.auth.auth as enaa
import emission.net.auth.skip as enas
import emission.net.auth.token_list as enat

# Test the auth methods. We will primarily test the "skip" and "token_list" 
# since they require no external integration

class TestAuthSelection(unittest.TestCase):
    def setUp(self):
        self.testUserUUID = uuid.uuid4()
        self.token_list_conf_path = "conf/net/auth/token_list.json"
        self.token_list_path = "conf/net/auth/token_list"
        etc.setupTokenListAuth(self)

        import shutil
        self.openid_auth_conf_path = "conf/net/auth/openid_auth.json"
        shutil.copyfile("%s.sample" % self.openid_auth_conf_path,
                        self.openid_auth_conf_path)
        with open(self.openid_auth_conf_path, "w") as fp:
            fp.write(json.dumps({
                "discoveryURI": "https://accounts.google.com/.well-known/openid-configuration",
                "clientID": "123456"
            }))

        self.google_auth_conf_path = "conf/net/auth/google_auth.json"
        shutil.copyfile("%s.sample" % self.google_auth_conf_path,
                        self.google_auth_conf_path)

    def tearDown(self):
        etc.tearDownTokenListAuth(self)
        os.remove(self.openid_auth_conf_path)
        os.remove(self.google_auth_conf_path)

    def testGetAuthMethod(self):
#        import emission.net.auth.openid_auth as enao
#        import emission.net.auth.google_auth as enag

        self.assertEqual(enaa.AuthMethodFactory.getAuthMethod("skip").__class__,
            enas.SkipMethod)
        self.assertEqual(enaa.AuthMethodFactory.getAuthMethod("token_list").__class__,
            enat.TokenListMethod)
# Commented out because this requires retrieval of data from a URL and is less
# likely to be reliable
#         self.assertEqual(enaa.AuthMethodFactory.getAuthMethod("openid_auth").__class__,
#             enao.OpenIDAuthMethod)
#         self.assertEqual(enaa.AuthMethodFactory.getAuthMethod("google_auth").__class__,
#             enag.GoogleAuthMethod)

    def testGetTokenInJSON(self):
        import emission.net.api.bottle as enab
        import io

        user_data = io.StringIO() 
        user_data.write(json.dumps({'user': "test_token"}))
        test_environ = etc.createDummyRequestEnviron(self, addl_headers=None, request_body=user_data)

        request = enab.LocalRequest(environ=test_environ)
        logging.debug("Found request body = %s" % request.body.getvalue())
        logging.debug("Found request headers = %s" % list(request.headers.keys()))
        self.assertEqual(enaa.__getToken__(request, inHeader=False), "test_token")
        with self.assertRaises(AttributeError):
            self.assertEqual(enaa.__getToken__(request, inHeader=True), "test_token")
        
    def testGetTokenInHeader(self):
        import emission.net.api.bottle as enab
        import io

        user_data = io.StringIO() 

        addl_headers = {'HTTP_USER': 'test_header_token'}

        test_environ = etc.createDummyRequestEnviron(self, addl_headers=addl_headers, request_body=user_data)
        request = enab.LocalRequest(environ=test_environ)
        logging.debug("Found request body = %s" % request.body.getvalue())
        logging.debug("Found request headers = %s" % list(request.headers.keys()))
        self.assertEqual(enaa.__getToken__(request, inHeader=True), "test_header_token")
        with self.assertRaises(TypeError):
            self.assertEqual(enaa.__getToken__(request, inHeader=False), "test_header_token")

    def testGetTokenInHeader(self):
        import emission.net.api.bottle as enab
        import io

        user_data = io.StringIO() 

        addl_headers = {'HTTP_USER': 'test_header_token'}

        test_environ = etc.createDummyRequestEnviron(self, addl_headers=addl_headers, request_body=user_data)
        request = enab.LocalRequest(environ=test_environ)
        logging.debug("Found request body = %s" % request.body.getvalue())
        logging.debug("Found request headers = %s" % list(request.headers.keys()))
        self.assertEqual(enaa.__getToken__(request, inHeader=True), "test_header_token")
        with self.assertRaises(TypeError):
            self.assertEqual(enaa.__getToken__(request, inHeader=False), "test_header_token")

    def testGetEmail(self):
        import emission.net.api.bottle as enab
        import io

        user_data = io.StringIO() 
        test_email = "correct_horse_battery_staple"
        user_data.write(json.dumps({'user': test_email}))
        test_environ = etc.createDummyRequestEnviron(self, addl_headers=None, request_body=user_data)

        request = enab.LocalRequest(environ=test_environ)
        logging.debug("Found request body = %s" % request.body.getvalue())
        logging.debug("Found request headers = %s" % list(request.headers.keys()))
        self.assertEqual(enaa._getEmail(request, "skip", inHeader=False), test_email)
        self.assertEqual(enaa._getEmail(request, "token_list", inHeader=False), test_email)
    def testGetUUIDSkipAuth(self):
        import emission.net.api.bottle as enab
        import emission.core.wrapper.user as ecwu
        import io

        self.test_email = "test_email"
        user_data = io.StringIO() 
        user_data.write(json.dumps({'user': self.test_email}))
        test_environ = etc.createDummyRequestEnviron(self, addl_headers=None, request_body=user_data)

        request = enab.LocalRequest(environ=test_environ)
        logging.debug("Found request body = %s" % request.body.getvalue())
        logging.debug("Found request headers = %s" % list(request.headers.keys()))
        user = ecwu.User.register(self.test_email)
        self.assertEqual(enaa.getUUID(request, "skip", inHeader=False), user.uuid)
        ecwu.User.unregister(self.test_email)

    def testGetUUIDTokenAuthSuccess(self):
        import emission.net.api.bottle as enab
        import emission.core.wrapper.user as ecwu
        import io

        self.test_email = "correct_horse_battery_staple"
        user_data = io.StringIO() 
        user_data.write(json.dumps({'user': self.test_email}))
        test_environ = etc.createDummyRequestEnviron(self, addl_headers=None,
                                                     request_body=user_data)

        request = enab.LocalRequest(environ=test_environ)
        logging.debug("Found request body = %s" % request.body.getvalue())
        logging.debug("Found request headers = %s" % list(request.headers.keys()))
        user = ecwu.User.register(self.test_email)
        self.assertEqual(enaa.getUUID(request, "token_list", inHeader=False), user.uuid)
        ecwu.User.unregister(self.test_email)

    def testGetUUIDTokenAuthFailure(self):
        import emission.net.api.bottle as enab
        import emission.core.wrapper.user as ecwu
        import io

        self.test_email = "correct_horse_battery_staple"
        user_data = io.StringIO() 
        user_data.write(json.dumps({'user': "incorrect_token"}))
        test_environ = etc.createDummyRequestEnviron(self, addl_headers=None, request_body=user_data)

        request = enab.LocalRequest(environ=test_environ)
        logging.debug("Found request body = %s" % request.body.getvalue())
        logging.debug("Found request headers = %s" % list(request.headers.keys()))
        user = ecwu.User.register(self.test_email)
        ecwu.User.unregister(self.test_email)
        with self.assertRaises(ValueError):
            self.assertEqual(enaa.getUUID(request, "token_list", inHeader=False), user.uuid)

if __name__ == '__main__':
    import emission.tests.common as etc

    etc.configLogging()
    unittest.main()
