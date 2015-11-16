# Standard imports
import unittest
import datetime as pydt
import logging
import json

# Our imports
import emission.core.get_database as edb
import emission.net.usercache.abstract_usercache as enua
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

# Test imports
import emission.tests.common as etc

class TestTripSegmentation(unittest.TestCase):
    def setUp(self):
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-27")
        self.androidUUID = self.testUUID
        etc.setupRealExample(self, "emission/tests/data/real_examples/iphone_2015-11-06")
        self.iosUUID = self.testUUID
        eaicf.filter_accuracy(self.iosUUID)
        estfm.move_all_filters_to_data()
        logging.debug("androidUUID = %s, iosUUID = %s" % (self.androidUUID, self.iosUUID))

    def tearDown(self):
        edb.get_timeseries_db().remove({"user_id": self.androidUUID}) 
        edb.get_timeseries_db().remove({"user_id": self.iosUUID}) 
        edb.get_place_db().remove() 
        edb.get_trip_new_db().remove() 
        
    def testSegmentationPointsDwellSegmentationTimeFilter(self):
        ts = esta.TimeSeries.get_time_series(self.androidUUID)
        tq = enua.UserCache.TimeQuery("write_ts", 1440658800, 1440745200)
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
                          1440700040.477, 1440719699.470, 1440723334.898, 1440729184.411])

    def testSegmentationPointsDwellSegmentationDistFilter(self):
        ts = esta.TimeSeries.get_time_series(self.iosUUID)
        tq = enua.UserCache.TimeQuery("write_ts", 1446796800, 1446847600)
        dstdsm = dsdf.DwellSegmentationDistFilter(time_threshold = 10 * 60, # 5 mins
                                                  point_threshold = 10,
                                                  distance_threshold = 100) # 100 m
        segmentation_points = dstdsm.segment_into_trips(ts, tq)
        for (start, end) in segmentation_points:
            logging.debug("trip is from %s (%f) -> %s (%f)" % (start.fmt_time, start.ts, end.fmt_time, end.ts))
        self.assertIsNotNone(segmentation_points)
        self.assertEqual(len(segmentation_points), 3)
        self.assertEqual([start.ts for (start, end) in segmentation_points],
                         [1446797042.282652, 1446821561.559255, 1446825879.933473])
        self.assertEqual([end.ts for (start, end) in segmentation_points],
                         [1446797923.682973, 1446825092.302420, 1446828217.125328])


    def testSegmentationWrapperAndroid(self):
        eaist.segment_current_trips(self.androidUUID)
        # The previous line should have created places and trips and stored
        # them into the database. Now, we want to query to ensure that they
        # were created correctly.
        tq_place = enua.UserCache.TimeQuery("enter_ts", 1440658800, 1440745200)
        created_places = esdp.get_places(self.androidUUID, tq_place)

        tq_trip = enua.UserCache.TimeQuery("start_ts", 1440658800, 1440745200)
        created_trips = esdt.get_trips(self.androidUUID, tq_trip)

        for i, place in enumerate(created_places):
            logging.debug("Retrieved places %s: %s -> %s" % (i, place.enter_fmt_time, place.exit_fmt_time))
        for i, trip in enumerate(created_trips):
            logging.debug("Retrieved trips %s: %s -> %s" % (i, trip.start_fmt_time, trip.end_fmt_time))

        # We expect there to be 9 places, but the first one is that start of
        # the chain, so it has a start_time of None and it won't be retrieved
        # by the query on the start_time that we show here.
        self.assertEqual(len(created_places), 8)
        self.assertEqual(len(created_trips), 8)

        # Pick the first two trips and the first place and ensure that they are all linked correctly
        # Note that this is the first place, not the second place because the true first place will not
        # be retrieved by the query, as shown above
        trip0 = created_trips[0]
        trip1 = created_trips[1]
        place0 = created_places[0]

        self.assertEqual(trip0.end_place, place0.get_id())
        self.assertEqual(trip1.start_place, place0.get_id())
        self.assertEqual(place0.ending_trip, trip0.get_id())
        self.assertEqual(place0.starting_trip, trip1.get_id())

        self.assertEqual(round(trip0.duration), 11 * 60 + 9)
        self.assertEqual(round(trip1.duration), 6 * 60 + 54)

        self.assertIsNotNone(place0.location)
        
    def testSegmentationWrapperIOS(self):
        eaist.segment_current_trips(self.iosUUID)
        # The previous line should have created places and trips and stored
        # them into the database. Now, we want to query to ensure that they
        # were created correctly.
        tq_place = enua.UserCache.TimeQuery("enter_ts", 1446796800, 1446847600)
        created_places = esdp.get_places(self.iosUUID, tq_place)

        tq_trip = enua.UserCache.TimeQuery("start_ts", 1446796800, 1446847600)
        created_trips = esdt.get_trips(self.iosUUID, tq_trip)

        for i, place in enumerate(created_places):
            logging.debug("Retrieved places %s: %s -> %s" % (i, place.enter_fmt_time, place.exit_fmt_time))
        for i, trip in enumerate(created_trips):
            logging.debug("Retrieved trips %s: %s -> %s" % (i, trip.start_fmt_time, trip.end_fmt_time))

        # We expect there to be 4 places, but the first one is that start of
        # the chain, so it has a start_time of None and it won't be retrieved
        # by the query on the start_time that we show here.
        self.assertEqual(len(created_places), 3)
        self.assertEqual(len(created_trips), 3)

        # Pick the first two trips and the first place and ensure that they are all linked correctly
        # Note that this is the first place, not the second place because the true first place will not
        # be retrieved by the query, as shown above
        # The first trip here is a dummy trip, so let's check the second and third trip instead
        trip0 = created_trips[1]
        trip1 = created_trips[2]
        place0 = created_places[1]

        self.assertEqual(trip0.end_place, place0.get_id())
        self.assertEqual(trip1.start_place, place0.get_id())
        self.assertEqual(place0.ending_trip, trip0.get_id())
        self.assertEqual(place0.starting_trip, trip1.get_id())

        self.assertEqual(round(trip0.duration), 58 * 60 + 51)
        self.assertEqual(round(trip1.duration), 38 * 60 + 57)

        self.assertIsNotNone(place0.location)
    
    def testSegmentationWrapperCombined(self):
        # Change iOS entries to have the android UUID
        for entry in esta.TimeSeries.get_time_series(self.iosUUID).find_entries():
            entry["user_id"] = self.androidUUID
            edb.get_timeseries_db().save(entry)
        
        # Now, segment the data for the combined UUID, which will include both
        # android and ios
        eaist.segment_current_trips(self.androidUUID)

        tq_place = enua.UserCache.TimeQuery("enter_ts", 1440658800, 1446847600)
        created_places = esdp.get_places(self.androidUUID, tq_place)

        tq_trip = enua.UserCache.TimeQuery("start_ts", 1440658800, 1446847600)
        created_trips = esdt.get_trips(self.androidUUID, tq_trip)

        for i, place in enumerate(created_places):
            logging.debug("Retrieved places %s: %s -> %s" % (i, place.enter_fmt_time, place.exit_fmt_time))
        for i, trip in enumerate(created_trips):
            logging.debug("Retrieved trips %s: %s -> %s" % (i, trip.start_fmt_time, trip.end_fmt_time))

        # We expect there to be 12 places, but the first one is that start of
        # the chain, so it has a start_time of None and it won't be retrieved
        # by the query on the start_time that we show here.
        self.assertEqual(len(created_places), 11)
        self.assertEqual(len(created_trips), 11)

        # Pick the first two trips and the first place and ensure that they are all linked correctly
        # Note that this is the first place, not the second place because the true first place will not
        # be retrieved by the query, as shown above
        # The first trip here is a dummy trip, so let's check the second and third trip instead
        trip0time = created_trips[0]
        trip1time = created_trips[1]
        place0time = created_places[0]
        
        self.assertEqual(trip0time.end_place, place0time.get_id())
        self.assertEqual(trip1time.start_place, place0time.get_id())
        self.assertEqual(place0time.ending_trip, trip0time.get_id())
        self.assertEqual(place0time.starting_trip, trip1time.get_id())

        self.assertEqual(round(trip0time.duration), 11 * 60 + 9)
        self.assertEqual(round(trip1time.duration), 6 * 60 + 54)

        self.assertIsNotNone(place0time.location)
        
        # There are 8 android trips first (index: 0-7).
        # index 8 is the short, bogus trip
        # So we want to check trips 9 and 10
        trip0dist = created_trips[9]
        trip1dist = created_trips[10]
        place0dist = created_places[9]
        
        self.assertEqual(trip0dist.end_place, place0dist.get_id())
        self.assertEqual(trip1dist.start_place, place0dist.get_id())
        self.assertEqual(place0dist.ending_trip, trip0dist.get_id())
        self.assertEqual(place0dist.starting_trip, trip1dist.get_id())

        self.assertEqual(round(trip0dist.duration), 58 * 60 + 51)
        self.assertEqual(round(trip1dist.duration), 38 * 60 + 57)

        self.assertIsNotNone(place0dist.location)
        

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
