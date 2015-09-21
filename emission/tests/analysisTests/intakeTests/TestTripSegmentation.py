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
import emission.core.wrapper.pipelinestate as ecwp

import emission.analysis.intake.segmentation.trip_segmentation_methods.dwell_segmentation_time_filter as dstf
import emission.analysis.intake.segmentation.trip_segmentation as eaist

import emission.storage.decorations.place_queries as esdp
import emission.storage.decorations.trip_queries as esdt

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
        
    def testSegmentationPointsDwellSegmentationTimeFilter(self):
        ts = esta.TimeSeries.get_time_series(self.testUUID)
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

    def testSegmentationWrapper(self):
        eaist.segment_current_trips(self.testUUID)
        # The previous line should have created places and trips and stored
        # them into the database. Now, we want to query to ensure that they
        # were created correctly.
        tq_place = enua.UserCache.TimeQuery("enter_ts", 1440658800, 1440745200)
        created_places = esdp.get_places(self.testUUID, tq_place)

        tq_trip = enua.UserCache.TimeQuery("start_ts", 1440658800, 1440745200)
        created_trips = esdt.get_trips(self.testUUID, tq_trip)

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

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
