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
        import shutil

        self.webserver_conf_path = "conf/net/api/webserver.conf"
        shutil.copyfile(
            "%s.sample" % self.webserver_conf_path, self.webserver_conf_path
        )
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

    def tearDown(self):
        os.remove(self.webserver_conf_path)

    def test404Redirect(self):
        from emission.net.api.bottle import response
        importlib.reload(enacw)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_header("Location"), None)

        enacw.error404("")
        self.assertEqual(response.status_code, 301)
        self.assertEqual(response.get_header("Location"), "http://somewhere.else")

    def testResolveAuth(self):
        self.assertEqual(enacw.resolve_auth("skip"),"skip")
        self.assertEqual(enacw.resolve_auth("token_list"),"token_list")
        self.assertEqual(enacw.resolve_auth("dynamic"),"token_list")
        self.assertNotEqual(enacw.resolve_auth("dynamic"),"skip")

    from unittest import mock
    @mock.patch.dict(os.environ, {"STUDY_CONFIG":"nrel-commute"}, clear=True)
    def test_ResolveAuthWithEnvVar(self):
        importlib.reload(enacw)
        self.assertEqual(enacw.resolve_auth("dynamic"),"skip")


if __name__ == "__main__":
    etc.configLogging()
    unittest.main()
