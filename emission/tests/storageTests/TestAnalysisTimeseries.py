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
import uuid
import json

# Our imports
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.abstract_timeseries as esta

import emission.core.get_database as edb
import emission.core.wrapper.rawtrip as ecwrt
import emission.core.wrapper.rawplace as ecwrp
import emission.core.wrapper.section as ecwc
import emission.core.wrapper.stop as ecws

import emission.tests.storageTests.analysis_ts_common as etsa

class TestAnalysisTimeseriesQueries(unittest.TestCase):
    def setUp(self):
        self.testUserId = uuid.uuid3(uuid.NAMESPACE_URL, "mailto:test@test.me")
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.testUserId})
        self.test_trip_id = "test_trip_id"

    def testCreateNew(self):
        etsa.createNewTripLike(self, esda.RAW_TRIP_KEY, ecwrt.Rawtrip)
        etsa.createNewPlaceLike(self, esda.RAW_PLACE_KEY, ecwrp.Rawplace)
        etsa.createNewTripLike(self, esda.RAW_SECTION_KEY, ecwc.Section)
        etsa.createNewPlaceLike(self, esda.RAW_STOP_KEY, ecws.Stop)

    def testSave(self):
        etsa.saveTripLike(self, esda.RAW_TRIP_KEY, ecwrt.Rawtrip)
        etsa.savePlaceLike(self, esda.RAW_PLACE_KEY, ecwrp.Rawplace)
        etsa.saveTripLike(self, esda.RAW_SECTION_KEY, ecwc.Section)
        etsa.savePlaceLike(self, esda.RAW_STOP_KEY, ecws.Stop)

    def testGet(self):
        etsa.getObject(self, esda.RAW_TRIP_KEY, ecwrt.Rawtrip)
        etsa.getObject(self, esda.RAW_PLACE_KEY, ecwrp.Rawplace)
        etsa.getObject(self, esda.RAW_SECTION_KEY, ecwc.Section)
        etsa.getObject(self, esda.RAW_STOP_KEY, ecws.Stop)

    def testQuery(self):
        etsa.queryTripLike(self, esda.RAW_TRIP_KEY, ecwrt.Rawtrip)
        etsa.queryPlaceLike(self, esda.RAW_PLACE_KEY, ecwrp.Rawplace)
        etsa.queryTripLike(self, esda.RAW_SECTION_KEY, ecwc.Section)
        etsa.queryPlaceLike(self, esda.RAW_STOP_KEY, ecws.Stop)

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
