# This tests REST API directly, and exercises code in the API wrapper layer
# It runs the test against a running webserver. The webserver host and port are
# read from the config.json file

import unittest
import json
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.DEBUG)

class TestCarbon(unittest.TestCase):
    def setUp(self):
        config_file = open('config.json')
        config_data = json.load(config_file)
        self.server_host = config_data["server"]["host"]
        self.server_port = config_data["server"]["port"]
        self.log_base_dir = config_data["paths"]["log_base_dir"]

