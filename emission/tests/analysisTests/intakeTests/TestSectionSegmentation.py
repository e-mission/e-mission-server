# Standard imports
import unittest
import datetime as pydt
import logging
import uuid
import json

# Our imports
import emission.core.get_database as edb
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.pipeline_queries as epq
import emission.core.wrapper.motionactivity as ecwm

import emission.analysis.intake.segmentation.section_segmentation_methods.smoothed_high_confidence_motion as shcm
import emission.analysis.intake.segmentation.section_segmentation as eaiss

import emission.analysis.intake.segmentation.trip_segmentation as eaist

import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.stop_queries as esdst
import emission.storage.decorations.section_queries as esds

class TestTripSegmentation(unittest.TestCase):
    def setUp(self):
        self.entries = json.load(open("emission/tests/data/my_data_jul_22.txt"))
        self.testUUID = uuid.uuid4()
        for entry in self.entries:
            entry["user_id"] = self.testUUID
            edb.get_timeseries_db().save(entry)

    def tearDown(self):
        edb.get_timeseries_db().remove({"user_id": self.testUUID}) 
        edb.get_place_db().remove() 
        edb.get_trip_new_db().remove() 
        
    def testSegmentationPointsSmoothedHighConfidenceMotion(self):
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        tq = enua.UserCache.TimeQuery("write_ts", 1440695152.989, 1440699266.669)
        shcmsm = shcm.SmoothedHighConfidenceMotion(60, [ecwm.MotionTypes.TILTING,
                                                        ecwm.MotionTypes.UNKNOWN,
                                                        ecwm.MotionTypes.STILL])
        segmentation_points = shcmsm.segment_into_sections(ts, tq)

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
        test_trip = esdt.create_new_trip(self.testUUID)
        test_trip.start_ts = 1440695152.989
        test_trip.start_fmt_time = "2015-08-27 10:05:52.989000-07:00"
        test_trip.start_loc = {
                "type": "Point",
                "coordinates": [
                    -122.4029569,
                    37.6162024
                ]
            }

        test_trip.end_ts = 1440699266.669
        test_trip.end_fmt_time = "2015-08-27 11:14:26.669000-07:00"
        test_trip.end_loc = {
                "type": "Point",
                "coordinates": [
                    -122.2603947,
                    37.875023
                ]
            }
        esdt.save_trip(test_trip)
        eaiss.segment_trip_into_sections(self.testUUID, test_trip.get_id())

        created_stops = esdt.get_stops_for_trip(self.testUUID, test_trip.get_id())
        created_sections = esdt.get_sections_for_trip(self.testUUID, test_trip.get_id())

        tq_stop = enua.UserCache.TimeQuery("enter_ts", 1440658800, 1440745200)
        queried_stops = esdst.get_stops(self.testUUID, tq_stop)

        tq_section = enua.UserCache.TimeQuery("start_ts", 1440658800, 1440745200)
        queried_sections = esds.get_sections(self.testUUID, tq_section)

        for i, stop in enumerate(created_stops):
            logging.info("Retrieved stop %s: %s -> %s" % (i, stop.enter_fmt_time, stop.exit_fmt_time))
        for i, section in enumerate(created_sections):
            logging.info("Retrieved section %s: %s -> %s" % (i, section.start_fmt_time, section.end_fmt_time))

        self.assertEqual(len(created_stops), 1)
        self.assertEqual(created_stops[0].enter_ts, 1440698066.704)
        self.assertEqual(created_stops[0].exit_ts, 1440698306.892)
        self.assertEqual(created_stops[0].ending_section, created_sections[0].get_id())
        self.assertEqual(created_stops[0].starting_section, created_sections[1].get_id())

        self.assertEqual(len(created_sections), 2)
        logging.info("Checking fields for section %s" % created_sections[0])
        self.assertEqual(created_sections[0].start_ts, 1440695152.989)
        self.assertEqual(created_sections[0].end_ts, 1440698066.704)
        self.assertIsNone(created_sections[0].start_stop)
        self.assertEqual(created_sections[0].end_stop, created_stops[0].get_id())

        logging.info("Checking fields for section %s" % created_sections[1])
        self.assertEqual(created_sections[1].start_ts, 1440698306.892)
        self.assertEqual(created_sections[1].end_ts, 1440699266.669)
        self.assertEqual(created_sections[1].start_stop, created_stops[0].get_id())
        self.assertIsNone(created_sections[1].end_stop)

        self.assertEqual(created_sections, queried_sections)
        self.assertEqual(created_stops, queried_stops)

    def testSegmentationWrapperWithAutoTrip(self):
        eaist.segment_current_trips(self.testUUID)

        tq_trip = enua.UserCache.TimeQuery("start_ts", 1440658800, 1440745200)
        created_trips = esdt.get_trips(self.testUUID, tq_trip)

        for i, trip in enumerate(created_trips):
            eaiss.segment_trip_into_sections(self.testUUID, trip.get_id())

            created_stops = esdt.get_stops_for_trip(self.testUUID, trip.get_id())
            created_sections = esdt.get_sections_for_trip(self.testUUID, trip.get_id())

            for j, stop in enumerate(created_stops):
                logging.info("Retrieved stops %s: %s -> %s" % (j, stop.enter_fmt_time, stop.exit_fmt_time))
            for j, section in enumerate(created_sections):
                logging.info("Retrieved sections %s: %s -> %s" % (j, section.start_fmt_time, section.end_fmt_time))

            # self.assertEqual(len(created_stops), 1)
            # self.assertEqual(len(created_sections), 2)


        # tq_stop = enua.UserCache.TimeQuery("enter_ts", 1440658800, 1440745200)
        # queried_stops = esdst.get_stops(self.testUUID, tq_stop)
        #
        # tq_section = enua.UserCache.TimeQuery("start_ts", 1440658800, 1440745200)
        # queried_sections = esds.get_sections(self.testUUID, tq_section)
        #
        # self.assertEqual(created_sections, queried_sections)
        # self.assertEqual(created_stops, queried_stops)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()
