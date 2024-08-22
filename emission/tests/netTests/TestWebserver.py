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
import shutil

# Our imports
import emission.tests.common as etc
import emission.net.api.cfc_webapp as enacw
import importlib

class TestWebserver(unittest.TestCase):
    def setUp(self):
        # Backwards_Compatibility method to read from config files 
        self.webserver_conf_path = "conf/net/api/webserver.conf"
        self.backup_file_path = "conf/net/api/webserver.conf.bak"
        
        if os.path.exists(self.webserver_conf_path):
            shutil.copy2(self.webserver_conf_path, self.backup_file_path)
        shutil.copyfile(f"{self.webserver_conf_path}.sample", self.webserver_conf_path)

        with open(self.webserver_conf_path, "w") as fd:
            fd.write(
                json.dumps(
                    {
                        "paths": {
                            "static_path": "webapp/www",
                            "python_path": "main",
                            "log_base_dir": ".",
                            "log_file": "debug.log",
                            "404_redirect": "http://somewhere.else",
                        },
                        "server": {
                            "host": "0.0.0.0",
                            "port": "8080",
                            "timeout": "3600",
                            "auth": "skip",
                            "aggregate_call_auth": "no_auth",
                        },
                    }
                )
            )
        logging.debug("Finished setting up %s" % self.webserver_conf_path)
        with open(self.webserver_conf_path) as fd:
            logging.debug("Current values are %s" % json.load(fd))

        # New method that uses environment variables only
        self.originalWebserverEnvVars = {}
        self.testModifiedEnvVars = {
            'WEBSERVER_NOT_FOUND_REDIRECT' : "http://somewhere.else"
        }

        self.orginalDBEnvVars = dict(os.environ)

        for env_var_name, env_var_value in self.testModifiedEnvVars.items():
            # Setting webserver environment variables with test values
            os.environ[env_var_name] = env_var_value

        logging.debug("Finished setting up test webserver environment variables")
        logging.debug("Current original values are = %s" % self.originalWebserverEnvVars)
        logging.debug("Current modified values are = %s" % self.testModifiedEnvVars)

    def tearDown(self):
        if os.path.exists(self.webserver_conf_path):
            os.remove(self.webserver_conf_path)
        
        if os.path.exists(self.backup_file_path):
            shutil.move(self.backup_file_path, self.webserver_conf_path)
            with open(self.webserver_conf_path, 'rb') as fd:
                restored_config = json.load(fd)
                logging.debug("Restored file contents: %s" % restored_config)
        
        logging.debug("Deleting test webserver environment variables")
        etc.restoreOriginalEnvVars(self.originalWebserverEnvVars,
            self.testModifiedEnvVars)
        logging.debug("Finished restoring original webserver environment variables")
        logging.debug("Restored original values are = %s" % self.originalWebserverEnvVars)

    def test404Redirect(self):
        from emission.net.api.bottle import response
        importlib.reload(enacw)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_header("Location"), None)

        enacw.error404("")
        self.assertEqual(response.status_code, 301)
        # self.assertEqual(response.get_header("Location"), "http://somewhere.else")

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
