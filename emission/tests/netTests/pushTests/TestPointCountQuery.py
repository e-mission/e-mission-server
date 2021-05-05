from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import logging
import json
import bson.json_util as bju
import attrdict as ad
import arrow
import numpy as np

# Our imports
import emission.core.get_database as edb
import emission.core.wrapper.localdate as ecwl

import emission.net.usercache.abstract_usercache_handler as enuah
import emission.net.ext_service.push.query.point_count as pointcount
import emission.storage.timeseries.tcquery as estt

# Test imports
import emission.tests.common as etc

class TestPointCountQuery(unittest.TestCase):
    def setUp(self):
        # Thanks to M&J for the number!
        np.random.seed(61297777)

    def tearDown(self):
        logging.debug("Clearing related databases")
        self.clearRelatedDb()

    def clearRelatedDb(self):
        edb.get_timeseries_db().delete_many({"user_id": {"$in": self.testUUIDList}})
        edb.get_analysis_timeseries_db().delete_many({"user_id": {"$in": self.testUUIDList}})
        edb.get_usercache_db().delete_many({"user_id": {"$in": self.testUUIDList}})

    def testAllQuery(self): 
        dataFile = "emission/tests/data/real_examples/shankari_2016-06-20"
        ld = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 20})

        etc.setupRealExample(self, dataFile)
        etc.runIntakePipeline(self.testUUID)
        self.testUUIDList = [self.testUUID]
        uuid_list = pointcount.query({
            "time_type": None,
            "modes": None,
            "sel_region": None
        })
        logging.debug("uuid_list = %s" % uuid_list)
        self.assertGreater(len(uuid_list), 1)
        self.assertIn(self.testUUID, uuid_list)

    def testTimeQueryPos(self): 
        dataFile = "emission/tests/data/real_examples/shankari_2016-06-20"
        ld = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 20})

        etc.setupRealExample(self, dataFile)
        etc.runIntakePipeline(self.testUUID)
        self.testUUIDList = [self.testUUID]

        uuid_list = pointcount.query({
            "time_type": "local_date",
            "from_local_date": dict(ld),
            "to_local_date": dict(ld),
            "modes": None,
            "sel_region": None
        })
        logging.debug("uuid_list = %s" % uuid_list)
        self.assertEqual(uuid_list, [self.testUUID])

    def testTimeQueryNeg(self): 
        # Load data for the 20th
        dataFile20 = "emission/tests/data/real_examples/shankari_2016-06-20"
        ld20 = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 20})

        etc.setupRealExample(self, dataFile20)
        testUUID20 = self.testUUID
        etc.runIntakePipeline(testUUID20)

        # Load data for the 21st
        dataFile21 = "emission/tests/data/real_examples/shankari_2016-06-21"
        ld21 = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 21})

        etc.setupRealExample(self, dataFile21)
        testUUID21 = self.testUUID
        etc.runIntakePipeline(testUUID21)

        self.testUUIDList = [testUUID20, testUUID21]

        uuid_list20 = pointcount.query({
            "time_type": "local_date",
            "from_local_date": dict(ld20),
            "to_local_date": dict(ld20),
            "modes": None,
            "sel_region": None
        })
        logging.debug("uuid_list for the 20th = %s" % uuid_list20)
       
        # We should only get uuid from the 20th back 
        self.assertEqual(uuid_list20, [testUUID20])

        uuid_list21 = pointcount.query({
            "time_type": "local_date",
            "from_local_date": dict(ld21),
            "to_local_date": dict(ld21),
            "modes": None,
            "sel_region": None
        })
        logging.debug("uuid_list for the 21st = %s" % uuid_list21)
       
        # We should only get uuid from the 21st back 
        self.assertEqual(uuid_list21, [testUUID21])

    def testGeoQuery(self):
        # Load data for the Bay Area
        dataFileba = "emission/tests/data/real_examples/shankari_2016-06-20"
        ldba = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 20})

        etc.setupRealExample(self, dataFileba)
        testUUIDba = self.testUUID
        etc.runIntakePipeline(testUUIDba)
        logging.debug("uuid for the bay area = %s " % testUUIDba)

        # Load data for Hawaii
        dataFilehi = "emission/tests/data/real_examples/shankari_2016-07-27"
        ldhi = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 27})

        etc.setupRealExample(self, dataFilehi)
        testUUIDhi = self.testUUID
        etc.runIntakePipeline(testUUIDhi)

        logging.debug("uuid for hawaii = %s " % testUUIDhi)

        self.testUUIDList = [testUUIDba, testUUIDhi]

        uuid_listba = pointcount.query({
            "time_type": "local_date",
            "from_local_date": {'year': 2016, 'month': 5, 'day': 20},
            "to_local_date": {'year': 2016, 'month': 10, 'day': 20},
            "modes": None,
            "sel_region": {
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                      [ [-122.0731149, 37.4003834], 
                        [-122.07302, 37.3804759],
                        [-122.1232527, 37.4105125],
                        [-122.1101028, 37.4199638],
                        [-122.0731149, 37.4003834] ]
                      ]
                }
            }
        })
        logging.debug("uuid_list for the bay area = %s" % uuid_listba)
       
        # We should only get uuid from the bay area back 
        self.assertEqual(uuid_listba, [testUUIDba])

        uuid_listhi = pointcount.query({
            "time_type": None,
            "modes": None,
            "sel_region": {
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                      [ [-157.9614841, 21.3631988],
                        [-157.9267982, 21.3780131],
                        [-157.7985052, 21.279961], 
                        [-157.8047025, 21.2561483],
                        [-157.9614841, 21.3631988] ]
                      ]
                }
            }
        })
        logging.debug("uuid_list for hawaii = %s" % uuid_listhi)
       
        # We should only get uuid from the 21st back 
        self.assertEqual(uuid_listhi, [testUUIDhi])



if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
