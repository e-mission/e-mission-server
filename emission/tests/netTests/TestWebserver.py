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
import emission.net.api.cfc_webapp as enacw
import importlib

class TestWebserver(unittest.TestCase):
    def setUp(self):
        self.originalWebserverEnvVars = {}
        self.testModifiedEnvVars = {
            'WEB_SERVER_OPENPATH_URL' : "http://somewhere.else"
        }

        for env_var_name, env_var_value in self.testModifiedEnvVars.items():
            if os.getenv(env_var_name) is not None:
                # Storing original webserver environment variables before modification
                self.originalWebserverEnvVars[env_var_name] = os.getenv(env_var_name)
                # Setting webserver environment variables with test values
                os.environ[env_var_name] = env_var_value

        logging.debug("Finished setting up test webserver environment variables")
        logging.debug("Current original values are = %s" % self.originalWebserverEnvVars)
        logging.debug("Current modified values are = %s" % self.testModifiedEnvVars)

    def tearDown(self):
        # Restoring original webserver environment variables
        for env_var_name, env_var_value in self.originalWebserverEnvVars.items():
            os.environ[env_var_name] = env_var_value
        logging.debug("Finished restoring original webserver environment variables")
        logging.debug("Restored original values are = %s" % self.originalWebserverEnvVars)

    def test404Redirect(self):
        from emission.net.api.bottle import response
        importlib.reload(enacw)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_header("Location"), None)

        enacw.error404("")
        self.assertEqual(response.status_code, 301)
        self.assertEqual(response.get_header("Location"), "http://somewhere.else")

    from unittest import mock
    @mock.patch.dict(os.environ, {"STUDY_CONFIG":"nrel-commute"}, clear=True)
    def test_ResolveAuthWithEnvVar(self):
        importlib.reload(enacw)
        self.assertEqual(enacw.resolve_auth("dynamic"),"skip")

    @mock.patch.dict(os.environ, {"STUDY_CONFIG":"denver-casr"}, clear=True)
    def test_ResolveAuthWithEnvVar(self):
        importlib.reload(enacw)
        self.assertEqual(enacw.resolve_auth("dynamic"),"skip")

    @mock.patch.dict(os.environ, {"STUDY_CONFIG":"stage-program"}, clear=True)
    def test_ResolveAuthWithEnvVar(self):
        importlib.reload(enacw)
        self.assertEqual(enacw.resolve_auth("dynamic"),"token_list")

    def testResolveAuthNoEnvVar(self):
        importlib.reload(enacw)
        self.assertEqual(enacw.resolve_auth("skip"),"skip")
        self.assertEqual(enacw.resolve_auth("token_list"),"token_list")
        self.assertEqual(enacw.resolve_auth("dynamic"),"token_list")
        self.assertNotEqual(enacw.resolve_auth("dynamic"),"skip")


if __name__ == "__main__":
    etc.configLogging()
    unittest.main()
