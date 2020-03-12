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
import emission.storage.decorations.place_queries as esdp
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda

import emission.core.get_database as edb
import emission.core.wrapper.rawplace as ecwrp

# Our test imports
import emission.tests.storageTests.analysis_ts_common as etsa

class TestPlaceQueries(unittest.TestCase):
    def setUp(self):
        self.testUserId = uuid.uuid3(uuid.NAMESPACE_URL, "mailto:test@test.me")
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.testUserId})

    def testGetLastPlace(self):
        old_place = ecwrp.Rawplace()
        old_place.enter_ts = 5
        old_place_id = esta.TimeSeries.get_time_series(
            self.testUserId).insert_data(
            self.testUserId, "segmentation/raw_place", old_place)
        old_place_entry = esda.get_entry(esda.RAW_PLACE_KEY, old_place_id)
        logging.debug("old place entry is %s "% old_place_entry)
        esta.TimeSeries.get_time_series(self.testUserId).update(old_place_entry)
        # The place saved in the previous step has no exit_ts set, so it is the
        # last place
        last_place_entry = esdp.get_last_place_entry(esda.RAW_PLACE_KEY,
                                                     self.testUserId)
        last_place_entry["data"]["exit_ts"] = 6
        logging.debug("About to update entry to %s" % last_place_entry)
        esta.TimeSeries.get_time_series(self.testUserId).update(last_place_entry)

        # Now that I have set the exit_ts and saved it, there is no last place
        last_place_entry = esdp.get_last_place_entry(esda.RAW_PLACE_KEY,
                                                     self.testUserId)
        self.assertIsNone(last_place_entry)

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
