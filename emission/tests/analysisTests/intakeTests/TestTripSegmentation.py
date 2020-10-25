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
import uuid
import bson.json_util as bju
import os

# Our imports
import emission.core.get_database as edb
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.pipeline_queries as epq
import emission.core.wrapper.pipelinestate as ecwp

import emission.analysis.intake.segmentation.trip_segmentation_methods.dwell_segmentation_time_filter as dstf
import emission.analysis.intake.segmentation.trip_segmentation_methods.dwell_segmentation_dist_filter as dsdf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.cleaning.filter_accuracy as eaicf

import emission.storage.timeseries.format_hacks.move_filter_field as estfm
import emission.storage.decorations.place_queries as esdp
import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.analysis_timeseries_queries as esda

# Test imports
import emission.tests.common as etc

class TestTripSegmentation(unittest.TestCase):
    def setUp(self):
        self.analysis_conf_path = \
            etc.set_analysis_config("intake.cleaning.filter_accuracy.enable", True)

        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-27")
        self.androidUUID = self.testUUID

        self.testUUID = uuid.UUID("c76a0487-7e5a-3b17-a449-47be666b36f6")
        with open("emission/tests/data/real_examples/iphone_2015-11-06") as fp:
            self.entries = json.load(fp, object_hook = bju.object_hook)
        etc.setupRealExampleWithEntries(self)
        self.iosUUID = self.testUUID
        eaicf.filter_accuracy(self.iosUUID)
        logging.debug("androidUUID = %s, iosUUID = %s" % (self.androidUUID, self.iosUUID))

    def tearDown(self):
        os.remove(self.analysis_conf_path)
        edb.get_timeseries_db().delete_many({"user_id": self.androidUUID}) 
        edb.get_timeseries_db().delete_many({"user_id": self.iosUUID})
        edb.get_pipeline_state_db().delete_many({"user_id": self.androidUUID})
        edb.get_pipeline_state_db().delete_many({"user_id": self.iosUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.androidUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.iosUUID}) 

    def testEmptyCall(self):
        import uuid
        dummyUserId = uuid.uuid4()
        # We just expect that this won't raise an exception
        eaist.segment_current_trips(dummyUserId)
        
    def testSegmentationPointsDwellSegmentationTimeFilter(self):
        ts = esta.TimeSeries.get_time_series(self.androidUUID)
        tq = estt.TimeQuery("metadata.write_ts", 1440658800, 1440745200)
        dstfsm = dstf.DwellSegmentationTimeFilter(time_threshold = 5 * 60, # 5 mins
                                                  point_threshold = 10,
                                                  distance_threshold = 100) # 100 m
        segmentation_points = dstfsm.segment_into_trips(ts, tq)
        for (start, end) in segmentation_points:
            logging.debug("trip is from %s (%f) -> %s (%f)" % (start.fmt_time, start.ts, end.fmt_time, end.ts))
        self.assertIsNotNone(segmentation_points)
        self.assertEqual(len(segmentation_points), 8)
        self.assertEqual([start.ts for (start, end) in segmentation_points],
                         [1440688739.672, 1440689662.943, 1440690718.768, 1440695152.989,
                          1440699933.687, 1440716367.376, 1440720239.012, 1440728519.971])
        self.assertEqual([end.ts for (start, end) in segmentation_points],
                         [1440689408.302, 1440690108.678, 1440694424.894, 1440699298.535,
                          1440700070.129, 1440719699.470, 1440723334.898, 1440729184.411])

    def testSegmentationPointsDwellSegmentationDistFilter(self):
        ts = esta.TimeSeries.get_time_series(self.iosUUID)
        tq = estt.TimeQuery("metadata.write_ts", 1446796800, 1446847600)
        dstdsm = dsdf.DwellSegmentationDistFilter(time_threshold = 10 * 60, # 5 mins
                                                  point_threshold = 10,
                                                  distance_threshold = 100) # 100 m
        segmentation_points = dstdsm.segment_into_trips(ts, tq)
        for (start, end) in segmentation_points:
            logging.debug("trip is from %s (%f) -> %s (%f)" % (start.fmt_time, start.ts, end.fmt_time, end.ts))
        self.assertIsNotNone(segmentation_points)
        self.assertEqual(len(segmentation_points), 2)
        self.assertEqual([start.ts for (start, end) in segmentation_points],
                         [1446797042.282652, 1446821561.559255])
        self.assertEqual([end.ts for (start, end) in segmentation_points],
                         [1446797923.682973, 1446828217.125328])


    def testSegmentationWrapperAndroid(self):
        eaist.segment_current_trips(self.androidUUID)
        # The previous line should have created places and trips and stored
        # them into the database. Now, we want to query to ensure that they
        # were created correctly.
        tq_place = estt.TimeQuery("data.enter_ts", 1440658800, 1440745200)
        created_places_entries = esda.get_entries(esda.RAW_PLACE_KEY,
                                                  self.androidUUID, tq_place)

        tq_trip = estt.TimeQuery("data.start_ts", 1440658800, 1440745200)
        created_trips_entries = esda.get_entries(esda.RAW_TRIP_KEY,
                                                 self.androidUUID, tq_trip)

        for i, place in enumerate(created_places_entries):
            logging.debug("Retrieved places %s: %s -> %s" % (i, place.data.enter_fmt_time, place.data.exit_fmt_time))
        for i, trip in enumerate(created_trips_entries):
            logging.debug("Retrieved trips %s: %s -> %s" % (i, trip.data.start_fmt_time, trip.data.end_fmt_time))

        # We expect there to be 9 places, but the first one is that start of
        # the chain, so it has a start_time of None and it won't be retrieved
        # by the query on the start_time that we show here.
        self.assertEqual(len(created_places_entries), 9)
        self.assertEqual(len(created_trips_entries), 8)

        # Pick the first two trips and the first place and ensure that they are all linked correctly
        # Note that this is the first place, not the second place because the true first place will not
        # be retrieved by the query, as shown above
        trip0 = created_trips_entries[0]
        trip1 = created_trips_entries[1]
        place0 = created_places_entries[0]

        self.assertEqual(trip0.data.end_place, place0.get_id())
        self.assertEqual(trip1.data.start_place, place0.get_id())
        self.assertEqual(place0.data.ending_trip, trip0.get_id())
        self.assertEqual(place0.data.starting_trip, trip1.get_id())

        self.assertEqual(round(trip0.data.duration), 11 * 60 + 9)
        self.assertEqual(round(trip1.data.duration), 6 * 60 + 54)

        self.assertIsNotNone(place0.data.location)
        
    def testSegmentationWrapperIOS(self):
        eaist.segment_current_trips(self.iosUUID)
        # The previous line should have created places and trips and stored
        # them into the database. Now, we want to query to ensure that they
        # were created correctly.
        tq_place = estt.TimeQuery("data.enter_ts", 1446796800, 1446847600)
        created_places_entries = esda.get_entries(esda.RAW_PLACE_KEY,
                                                  self.iosUUID, tq_place)

        tq_trip = estt.TimeQuery("data.start_ts", 1446796800, 1446847600)
        created_trips_entries = esda.get_entries(esda.RAW_TRIP_KEY,
                                                 self.iosUUID, tq_trip)

        for i, place in enumerate(created_places_entries):
            logging.debug("Retrieved places %s: %s -> %s" % (i, place.data.enter_fmt_time, place.data.exit_fmt_time))
        for i, trip in enumerate(created_trips_entries):
            logging.debug("Retrieved trips %s: %s -> %s" % (i, trip.data.start_fmt_time, trip.data.end_fmt_time))

        # We expect there to be 4 places, but the first one is that start of
        # the chain, so it has a start_time of None and it won't be retrieved
        # by the query on the start_time that we show here.
        self.assertEqual(len(created_places_entries), 2)
        self.assertEqual(len(created_trips_entries), 2)

        # Pick the first two trips and the first place and ensure that they are all linked correctly
        # Note that this is the first place, not the second place because the true first place will not
        # be retrieved by the query, as shown above
        # The first trip here is a dummy trip, so let's check the second and third trip instead
        trip0 = created_trips_entries[0]
        trip1 = created_trips_entries[1]
        place0 = created_places_entries[0]

        self.assertEqual(trip0.data.end_place, place0.get_id())
        self.assertEqual(trip1.data.start_place, place0.get_id())
        self.assertEqual(place0.data.ending_trip, trip0.get_id())
        self.assertEqual(place0.data.starting_trip, trip1.get_id())

        self.assertEqual(round(trip0.data.duration), 14 * 60 + 41)
        self.assertEqual(round(trip1.data.duration), 1 * 60 * 60 + 50 * 60 + 56)

        self.assertIsNotNone(place0.data.location)
    
    def testSegmentationWrapperCombined(self):
        # Change iOS entries to have the android UUID
        tsdb = edb.get_timeseries_db()
        for entry in esta.TimeSeries.get_time_series(
                self.iosUUID).find_entries():
            entry["user_id"] = self.androidUUID
            edb.save(tsdb, entry)
        
        # Now, segment the data for the combined UUID, which will include both
        # android and ios
        eaist.segment_current_trips(self.androidUUID)

        tq_place = estt.TimeQuery("data.enter_ts", 1440658800, 1446847600)
        created_places_entries = esda.get_entries(esda.RAW_PLACE_KEY,
                                                  self.androidUUID, tq_place)

        tq_trip = estt.TimeQuery("data.start_ts", 1440658800, 1446847600)
        created_trips_entries = esda.get_entries(esda.RAW_TRIP_KEY,
                                                 self.androidUUID, tq_trip,
                                                 untracked_key=esda.RAW_UNTRACKED_KEY)

        for i, place in enumerate(created_places_entries):
            logging.debug("Retrieved places %s: %s -> %s" % (i, place.data.enter_fmt_time, place.data.exit_fmt_time))
        for i, trip in enumerate(created_trips_entries):
            logging.debug("Retrieved trips %s: %s -> %s" % (i, trip.data.start_fmt_time, trip.data.end_fmt_time))

        # We expect there to be 12 places, but the first one is that start of
        # the chain, so it has a start_time of None and it won't be retrieved
        # by the query on the start_time that we show here.
        self.assertEqual(len(created_places_entries), 11)
        self.assertEqual(len(created_trips_entries), 11)

        # Pick the first two trips and the first place and ensure that they are all linked correctly
        # Note that this is the first place, not the second place because the true first place will not
        # be retrieved by the query, as shown above
        # The first trip here is a dummy trip, so let's check the second and third trip instead
        trip0time = created_trips_entries[0]
        trip1time = created_trips_entries[1]
        place0time = created_places_entries[0]
        
        self.assertEqual(trip0time.data.end_place, place0time.get_id())
        self.assertEqual(trip1time.data.start_place, place0time.get_id())
        self.assertEqual(place0time.data.ending_trip, trip0time.get_id())
        self.assertEqual(place0time.data.starting_trip, trip1time.get_id())

        self.assertEqual(round(trip0time.data.duration), 11 * 60 + 9)
        self.assertEqual(round(trip1time.data.duration), 6 * 60 + 54)

        self.assertIsNotNone(place0time.data.location)
        
        # There are 9 android "trips" first (index: 0-8), including the untracked time
        # index 9 is the short, bogus trip
        # So we want to check trips 10 and 11
        trip0dist = created_trips_entries[9]
        trip1dist = created_trips_entries[10]
        place0dist = created_places_entries[9]
        
        self.assertEqual(trip0dist.data.end_place, place0dist.get_id())
        self.assertEqual(trip1dist.data.start_place, place0dist.get_id())
        self.assertEqual(place0dist.data.ending_trip, trip0dist.get_id())
        self.assertEqual(place0dist.data.starting_trip, trip1dist.get_id())

        self.assertEqual(round(trip0dist.data.duration), 14 * 60 + 41)
        self.assertEqual(round(trip1dist.data.duration), 1 * 60 * 60 + 50 * 60 + 56)

        self.assertIsNotNone(place0dist.data.location)
        

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
