from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import range
from builtins import *
import unittest
import datetime as pydt
import logging
import pymongo
import json
import bson.json_util as bju
import pandas as pd
from uuid import UUID

# Our imports
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.analysis.intake.cleaning.clean_and_resample as eaicc

import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.location as ecwl

import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.intake.cleaning.clean_and_resample as eaicr

import emission.tests.common as etc

class TestCleanAndResample(unittest.TestCase):
    def setUp(self):
        import emission.core.get_database as edb
        import uuid
        self.testUUID = uuid.uuid4()
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)

    def tearDown(self):
        import emission.core.get_database as edb
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})

    def testRemoveOutliers(self):
        TS_START = 12345
        for i in range(0,10):
            dummy_loc = ecwl.Location({
                "ts": TS_START + i,
                "lat": 50 + i,
                "lng": 180 + i
            })
            self.ts.insert(ecwe.Entry.create_entry(self.testUUID,
                                                   "background/filtered_location",
                                                   dummy_loc))

        tq = estt.TimeQuery("data.ts", TS_START - 10, TS_START + 10 + 10)
        loc_entries = list(self.ts.find_entries(["background/filtered_location"], tq))
        loc_df = self.ts.get_data_df("background/filtered_location", tq)
        filtered_loc_df = eaicc.remove_outliers(loc_entries, loc_df["_id"])
        self.assertEqual(len(loc_entries), len(loc_df))
        self.assertEqual(len(filtered_loc_df), 0)

    def testRemoveAllOutliers(self):
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2016-06-20")
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)
        eaist.segment_current_trips(self.testUUID)
        eaiss.segment_current_sections(self.testUUID)
        eaicl.filter_current_sections(self.testUUID)
        # get all sections
        sections = [ecwe.Entry(s) for s in self.ts.find_entries([esda.RAW_SECTION_KEY], time_query=None)]
        for section in sections:
            filtered_points_entry_doc = self.ts.get_entry_at_ts("analysis/smoothing",
                                                           "data.section",
                                                           section.get_id())
            if filtered_points_entry_doc is not None:
                logging.debug("Found smoothing result for section %s" % section.get_id())
                # Setting the set of deleted points to everything
                loc_tq = esda.get_time_query_for_trip_like(esda.RAW_SECTION_KEY, section.get_id())
                loc_df = self.ts.get_data_df("background/filtered_location", loc_tq)
                filtered_points_entry_doc["data"]["deleted_points"] = loc_df["_id"].tolist()
                self.ts.update(ecwe.Entry(filtered_points_entry_doc))

        # All we care is that this should not crash.
        eaicr.clean_and_resample(self.testUUID)

        # Most of the trips have zero length, but apparently one has non-zero length
        # because the stop length is non zero!!
        # So there is only one cleaned trip left
        cleaned_trips_df = self.ts.get_data_df(esda.CLEANED_TRIP_KEY, time_query=None)
        logging.debug("non-zero length trips = %s" % cleaned_trips_df[["start_fmt_time", "end_fmt_time"]])
        self.assertEqual(len(cleaned_trips_df), 2)

        # We don't support squishing sections, but we only store stops and sections
        # for non-squished trips. And this non-squished trip happens to have
        # two sections and one stop
        cleaned_sections_df = self.ts.get_data_df(esda.CLEANED_SECTION_KEY, time_query=None)
        self.assertEqual(len(cleaned_sections_df), 4)
        # The first section is 3252.023643502053 
        self.assertAlmostEqual(cleaned_sections_df.distance.tolist()[0], 3252, 0)
        self.assertEqual(cleaned_sections_df.distance.tolist()[1:], [0,0,0])

        cleaned_stops_df = self.ts.get_data_df(esda.CLEANED_STOP_KEY, time_query=None)
        self.assertEqual(len(cleaned_stops_df), 2)
        self.assertAlmostEqual(cleaned_stops_df.distance[0], 0, places=0)
        self.assertAlmostEqual(cleaned_stops_df.distance[1], 150, places=0)


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
