from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import datetime as pydt
import logging
import json
import copy
import uuid

# Test imports
import emission.tests.common as etc
import emission.analysis.configs.config as eacc

import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.format_hacks.move_filter_field as estfm
import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.core.get_database as edb

import emission.tests.common as etc

class TestSaveAllConfigs(unittest.TestCase):
    def setUp(self):
        self.androidUUID = uuid.uuid4()
        self.iosUUID = uuid.uuid4()
        self.dummy_config = {'user_id': self.androidUUID,
                             'metadata': {
                                'key': 'config/sensor_config'
                              }, 'data': {
                                'is_duty_cycling': True
                              }
                            }
        logging.debug("androidUUID = %s, iosUUID = %s" % (self.androidUUID, self.iosUUID))

    def tearDown(self):
        edb.get_timeseries_db().delete_many({"user_id": self.androidUUID}) 
        edb.get_timeseries_db().delete_many({"user_id": self.iosUUID}) 
        edb.get_usercache_db().delete_many({"user_id": self.androidUUID}) 
        edb.get_usercache_db().delete_many({"user_id": self.iosUUID}) 
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.androidUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.iosUUID})

    def testNoOverrides(self):
        tq = estt.TimeQuery("metadata.write_ts", 1440658800, 1440745200)
        eacc.save_all_configs(self.androidUUID, tq)
        saved_entries = list(edb.get_usercache_db().find({'user_id': self.androidUUID, 'metadata.key': 'config/sensor_config'}))
        self.assertEqual(len(saved_entries), 0)

    def testOneOverride(self):
        cfg_1 = copy.copy(self.dummy_config)
        cfg_1['metadata']['write_ts'] = 1440700000
        edb.get_timeseries_db().insert_one(cfg_1)

        tq = estt.TimeQuery("metadata.write_ts", 1440658800, 1440745200)
        eacc.save_all_configs(self.androidUUID, tq)
        saved_entries = list(edb.get_usercache_db().find({'user_id': self.androidUUID, 'metadata.key': 'config/sensor_config'}))
        self.assertEqual(len(saved_entries), 1)
        logging.debug(saved_entries[0])
        self.assertEqual(saved_entries[0]['data']['is_duty_cycling'], cfg_1['data']['is_duty_cycling'])

    def testTwoOverride(self):
        cfg_1 = copy.copy(self.dummy_config)
        cfg_1['metadata']['write_ts'] = 1440700000
        edb.get_timeseries_db().insert_one(cfg_1)

        cfg_2 = copy.copy(self.dummy_config)
        cfg_2['metadata']['write_ts'] = 1440710000
        cfg_2['data']['is_duty_cycling'] = False
        edb.get_timeseries_db().insert_one(cfg_2)

        tq = estt.TimeQuery("metadata.write_ts", 1440658800, 1440745200)
        eacc.save_all_configs(self.androidUUID, tq)
        saved_entries = list(edb.get_usercache_db().find({'user_id': self.androidUUID, 'metadata.key': 'config/sensor_config'}))
        self.assertEqual(len(saved_entries), 1)
        logging.debug(saved_entries[0])
        self.assertEqual(saved_entries[0]['data']['is_duty_cycling'], cfg_2['data']['is_duty_cycling'])

    def testOldOverride(self):
        cfg_1 = copy.copy(self.dummy_config)
        cfg_1['metadata']['write_ts'] = 1440500000
        edb.get_timeseries_db().insert_one(cfg_1)

        cfg_2 = copy.copy(self.dummy_config)
        cfg_2['metadata']['write_ts'] = 1440610000
        edb.get_timeseries_db().insert_one(cfg_2)

        tq = estt.TimeQuery("metadata.write_ts", 1440658800, 1440745200)
        eacc.save_all_configs(self.androidUUID, tq)
        saved_entries = list(edb.get_usercache_db().find({'user_id': self.androidUUID, 'metadata.key': 'config/sensor_config'}))
        self.assertEqual(len(saved_entries), 0)
        
if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
