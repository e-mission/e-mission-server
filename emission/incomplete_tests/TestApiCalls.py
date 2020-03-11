from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# This tests REST API directly, and exercises code in the API wrapper layer
# It runs the test against a running webserver. The webserver host and port are
# read from the config.json file

from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import json
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.DEBUG)

class TestCarbon(unittest.TestCase):
    def setUp(self):
        config_file = open('config.json')
        config_data = json.load(config_file)
        config_file.close()
        self.server_host = config_data["server"]["host"]
        self.server_port = config_data["server"]["port"]
        self.log_base_dir = config_data["paths"]["log_base_dir"]

