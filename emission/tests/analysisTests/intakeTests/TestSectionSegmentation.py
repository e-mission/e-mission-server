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
import bson.json_util as bju
import uuid
import os

# Our imports
import emission.core.get_database as edb
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.abstract_timeseries as esta

import emission.core.wrapper.motionactivity as ecwm
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.rawtrip as ecwrt
import emission.core.wrapper.rawplace as ecwrp
import emission.core.wrapper.pipelinestate as ecwp

import emission.analysis.intake.segmentation.section_segmentation_methods.smoothed_high_confidence_motion as shcm
import emission.analysis.intake.segmentation.section_segmentation as eaiss

import emission.analysis.intake.segmentation.trip_segmentation as eaist

import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.pipeline_queries as esp

import emission.analysis.intake.cleaning.filter_accuracy as eaicf

# Test imports
import emission.tests.common as etc

class TestSectionSegmentation(unittest.TestCase):
    def setUp(self):
        self.analysis_conf_path = \
            etc.set_analysis_config("intake.cleaning.filter_accuracy.enable", True)

        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-27")
        self.androidUUID = self.testUUID
        eaicf.filter_accuracy(self.androidUUID)

        self.testUUID = uuid.UUID("c76a0487-7e5a-3b17-a449-47be666b36f6")
        with open("emission/tests/data/real_examples/iphone_2015-11-06") as fp:
            self.entries = json.load(fp, object_hook = bju.object_hook)
        etc.setupRealExampleWithEntries(self)
        self.iosUUID = self.testUUID
        eaicf.filter_accuracy(self.iosUUID)

    def tearDown(self):
        self.clearRelatedDb()
        os.remove(self.analysis_conf_path)

    def clearRelatedDb(self):
        edb.get_timeseries_db().remove({"user_id": self.androidUUID})
        edb.get_analysis_timeseries_db().remove({"user_id": self.androidUUID})
        edb.get_pipeline_state_db().remove({"user_id": self.androidUUID})
        edb.get_timeseries_db().remove({"user_id": self.iosUUID})
        edb.get_analysis_timeseries_db().remove({"user_id": self.iosUUID})
        edb.get_pipeline_state_db().remove({"user_id": self.iosUUID})

    def testSegmentationPointsSmoothedHighConfidenceMotion(self):
        ts = esta.TimeSeries.get_time_series(self.androidUUID)
        tq = estt.TimeQuery("metadata.write_ts", 1440695152.989, 1440699266.669)
        shcmsm = shcm.SmoothedHighConfidenceMotion(60, 100, [ecwm.MotionTypes.TILTING,
                                                        ecwm.MotionTypes.UNKNOWN,
                                                        ecwm.MotionTypes.STILL])
        segmentation_points = shcmsm.segment_into_sections(ts, 0, tq)

        for (start, end, motion) in segmentation_points:
            logging.info("section is from %s (%f) -> %s (%f) using mode %s" %
                (start.fmt_time, start.ts, end.fmt_time, end.ts, motion))
        self.assertIsNotNone(segmentation_points)
        self.assertEqual(len(segmentation_points), 2)
        self.assertEqual([start.ts for (start, end, motion) in segmentation_points],
                          [1440695873.453, 1440698306.892])
        self.assertEqual([end.ts for (start, end, motion) in segmentation_points],
                          [1440698066.704, 1440699234.834])

    def testSegmentationWrapperWithManualTrip(self):
        ts = esta.TimeSeries.get_time_series(self.androidUUID)
        test_trip = ecwrt.Rawtrip()
        test_trip.start_ts = 1440695152.989
        test_trip.start_fmt_time = "2015-08-27 10:05:52.989000-07:00"
        test_trip.start_local_dt = {
            'year': 2015,
            'month': 8,
            'day': 27,
            'hour': 10,
            'minute': 5,
            'second': 52,
            'timezone': "America/Los_Angeles"
        }
        test_trip.start_loc = {
                "type": "Point",
                "coordinates": [
                    -122.4029569,
                    37.6162024
                ]
            }

        test_trip.end_ts = 1440699266.669
        test_trip.end_fmt_time = "2015-08-27 11:14:26.669000-07:00"
        test_trip.end_local_dt = {
            'year': 2015,
            'month': 8,
            'day': 27,
            'hour': 11,
            'minute': 14,
            'second': 26,
            'timezone': "America/Los_Angeles"
        }
        test_trip.end_loc = {
                "type": "Point",
                "coordinates": [
                    -122.2603947,
                    37.875023
                ]
            }

        test_place = ecwrp.Rawplace()
        test_place.location = test_trip.start_loc
        test_place.exit_ts = test_trip.start_ts
        test_place.exit_local_dt = test_trip.start_local_dt
        test_place.exit_fmt_time = test_trip.start_fmt_time
        test_place_entry = ecwe.Entry.create_entry(self.androidUUID,
            "segmentation/raw_place", test_place)
        test_trip.start_place = test_place_entry.get_id()

        test_trip_id = ts.insert(ecwe.Entry.create_entry(self.androidUUID,
            "segmentation/raw_trip", test_trip))
        test_trip_entry = ts.get_entry_from_id(esda.RAW_TRIP_KEY, test_trip_id)
        test_place.starting_trip = test_trip_id
        ts.insert(test_place_entry)

        eaiss.segment_trip_into_sections(self.androidUUID, test_trip_entry, "DwellSegmentationTimeFilter")

        created_stops_entries = esdt.get_raw_stops_for_trip(self.androidUUID, test_trip_id)
        created_sections_entries = esdt.get_raw_sections_for_trip(self.androidUUID, test_trip_id)
        created_stops = [entry.data for entry in created_stops_entries]
        created_sections = [entry.data for entry in created_sections_entries]

        tq_stop = estt.TimeQuery("data.enter_ts", 1440658800, 1440745200)
        queried_stops = esda.get_objects(esda.RAW_STOP_KEY, self.androidUUID, tq_stop)

        tq_section = estt.TimeQuery("data.start_ts", 1440658800, 1440745200)
        queried_sections = esda.get_objects(esda.RAW_SECTION_KEY, self.androidUUID, tq_section)

        for i, stop in enumerate(created_stops):
            logging.info("Retrieved stop %s: %s -> %s" % (i, stop.enter_fmt_time, stop.exit_fmt_time))
        for i, section in enumerate(created_sections):
            logging.info("Retrieved section %s: %s -> %s" % (i, section.start_fmt_time, section.end_fmt_time))

        self.assertEqual(len(created_stops), 1)
        self.assertEqual(created_stops[0].enter_ts, 1440698066.704)
        self.assertEqual(created_stops[0].exit_ts, 1440698306.892)
        self.assertEqual(created_stops[0].exit_loc, created_sections[1].start_loc)
        self.assertEqual(created_stops[0].ending_section, created_sections_entries[0].get_id())
        self.assertEqual(created_stops[0].starting_section, created_sections_entries[1].get_id())

        self.assertEqual(len(created_sections), 2)
        logging.info("Checking fields for section %s" % created_sections[0])
        self.assertEqual(created_sections[0].start_ts, 1440695152.989)
        self.assertEqual(created_sections[0].end_ts, 1440698066.704)
        self.assertIsNone(created_sections[0].start_stop)
        self.assertEqual(created_sections[0].end_stop, created_stops_entries[0].get_id())

        logging.info("Checking fields for section %s" % created_sections[1])
        self.assertEqual(created_sections[1].start_ts, 1440698306.892)
        self.assertEqual(created_sections[1].end_ts, 1440699266.669)
        self.assertEqual(created_sections[1].start_stop, created_stops_entries[0].get_id())
        self.assertIsNone(created_sections[1].end_stop)

        self.assertEqual(created_sections, queried_sections)
        self.assertEqual(created_stops, queried_stops)

    def testSegmentationWrapperWithAutoTrip(self):
        eaist.segment_current_trips(self.androidUUID)
        eaiss.segment_current_sections(self.androidUUID)

        tq_trip = estt.TimeQuery("data.start_ts", 1440658800, 1440745200)
        created_trips = esda.get_entries(esda.RAW_TRIP_KEY, self.androidUUID,
                                         tq_trip)

        self.assertEqual(len(created_trips), 8)

        sections_stops = [(len(esdt.get_raw_sections_for_trip(self.androidUUID, trip.get_id())),
                           len(esdt.get_raw_stops_for_trip(self.androidUUID, trip.get_id())))
                          for trip in created_trips]
        logging.debug(sections_stops)
        self.assertEqual(len(sections_stops), len(created_trips))
        # The expected value was copy-pasted from the debug statement above
        self.assertEqual(sections_stops,
                         [(2, 1), (1, 0), (2, 1), (2, 1), (1, 0), (2, 1),
                          (4, 3), (2, 1)])

        # tq_stop = estt.TimeQuery("data.enter_ts", 1440658800, 1440745200)
        # queried_stops = esdst.get_stops(self.testUUID, tq_stop)
        #
        # tq_section = estt.TimeQuery("data.start_ts", 1440658800, 1440745200)
        # queried_sections = esds.get_sections(self.testUUID, tq_section)
        #
        # self.assertEqual(created_sections, queried_sections)
        # self.assertEqual(created_stops, queried_stops)

    def testIOSSegmentationWrapperWithAutoTrip(self):
        eaist.segment_current_trips(self.iosUUID)
        eaiss.segment_current_sections(self.iosUUID)

        tq_trip = estt.TimeQuery("data.start_ts", 1446700000, 1446900000)
        created_trips = esda.get_entries(esda.RAW_TRIP_KEY, self.iosUUID,
                                         tq_trip)

        self.assertEqual(len(created_trips), 2)
        logging.debug("created trips = %s" % created_trips)

        sections_stops = [(len(esdt.get_raw_sections_for_trip(self.iosUUID, trip.get_id())),
                           len(esdt.get_raw_stops_for_trip(self.iosUUID, trip.get_id())))
                          for trip in created_trips]
        logging.debug(sections_stops)
        self.assertEqual(len(sections_stops), len(created_trips))
        # The expected value was copy-pasted from the debug statement above
        self.assertEqual(sections_stops,
                         [(0, 0), (11, 10)])


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
