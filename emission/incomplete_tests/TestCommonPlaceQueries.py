from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import unittest
import uuid
import geojson as gj
import datetime
import numpy as np

import emission.storage.decorations.common_place_queries as esdcpq
import emission.analysis.modelling.tour_model.cluster_pipeline as eamtcp
import emission.simulation.trip_gen as tg
import emission.core.get_database as edb
import emission.storage.decorations.common_trip_queries as esdctp
import emission.tests.common as etc
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.storage.timeseries.format_hacks.move_filter_field as estfm

class TestCommonPlaceQueries(unittest.TestCase):
    
    def setUp(self):
        edb.get_common_place_db().drop()
        edb.get_common_trip_db().drop()
        self.testUserId = uuid.uuid4()
        self.testLocation = gj.Point((122.1234, 37.1234))
        self.testEnd = esdcpq.make_new_common_place(uuid.uuid4(), gj.Point((1,2.092)))
        esdcpq.save_common_place(self.testEnd)
        self.testEnd = esdcpq.get_common_place_at_location(self.testEnd.location)
        self.testEnd.successors = ()

        self.testStart = esdcpq.make_new_common_place(uuid.uuid4(), gj.Point((1,2)))
        self.testStart.successors = (self.testEnd.get_id(),)

        esdcpq.save_common_place(self.testEnd)
        esdcpq.save_common_place(self.testStart)
        self.time0 = datetime.datetime(1900, 1, 1, 1)

    def tearDown(self):
        edb.get_common_trip_db().drop()
        edb.get_common_place_db().drop()

    def testCreation(self):
        place = esdcpq.make_new_common_place(self.testUserId, self.testLocation)
        place.successors = ()
        self.assertEqual(type(place.location), gj.Point)
        self.assertEqual(type(place.successors), tuple)
        self.assertIsNotNone(place.successors)

    def testCreatePlace(self):
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-27")
        eaicf.filter_accuracy(self.testUUID)
        estfm.move_all_filters_to_data()
        eaist.segment_current_trips(self.testUUID)
        eaiss.segment_current_sections(self.testUUID)
        data = eamtcp.main(self.testUUID)
        esdcpq.create_places(data, self.testUUID)
        places = esdcpq.get_all_common_places_for_user(self.testUUID)
        places_list = []
        for p in places:
            places_list.append(esdcpq.make_common_place(p))
        for place in places_list:
            self.assertIsNotNone(place.location)
            self.assertIsNotNone(place["successors"])

    def testGetSuccessor(self):
        logging.debug("size of db is %s" % edb.get_common_place_db().find().count())
        self.assertIsNotNone(edb.get_common_place_db().find_one({"_id": self.testEnd.get_id()}))
        probs = np.zeros( (7, 24) )
        probs[self.time0.weekday(), 3] = 10
        props = {
            "user_id" : self.testUserId,
            "start_place" : self.testStart.get_id(),
            "end_place" : self.testEnd.get_id(),
            "start_loc" : self.testStart.location,
            "end_loc" : self.testEnd.location,
            "probabilites" : probs,
            "trips" : (),
            "start_times": [],
            "durations": []
        }
        trip = esdctp.make_new_common_trip(props)
        esdctp.save_common_trip(trip)
        suc = esdcpq.get_succesor(self.testUserId, self.testStart.get_id(), self.time0)
        self.assertEqual(suc, self.testEnd.get_id())




if __name__ == "__main__":
    unittest.main()
