# Standard imports
import unittest
import datetime as pydt
import logging
import uuid
import json
import pymongo
import geojson as gj

# Our imports
import emission.core.get_database as edb
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.pipeline_queries as epq
import emission.core.wrapper.motionactivity as ecwm

import emission.analysis.plotting.geojson.geojson_feature_converter as gjfc
import emission.analysis.intake.segmentation.section_segmentation as eaiss

import emission.analysis.intake.segmentation.trip_segmentation as eaist

import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.stop_queries as esdst
import emission.storage.decorations.section_queries as esds
import emission.storage.decorations.timeline as esdtl

class TestGeojsonFeatureConverter(unittest.TestCase):
    def setUp(self):
        self.clearRelatedDb()
        logging.info("Before loading, timeseries db size = %s" % edb.get_timeseries_db().count())
        self.entries = json.load(open("emission/tests/data/my_data_jul_22.txt"))
        self.testUUID = uuid.uuid4()
        for entry in self.entries:
            entry["user_id"] = self.testUUID
            # print "Saving entry with write_ts = %s and ts = %s" % (entry["metadata"]["write_fmt_time"],
            #                                                        entry["data"]["fmt_time"])
            edb.get_timeseries_db().save(entry)
        logging.info("After loading, timeseries db size = %s" % edb.get_timeseries_db().count())
        logging.debug("First few entries = %s" % [e["data"]["fmt_time"] for e in
                                                  list(edb.get_timeseries_db().find().sort("data.write_ts",
                                                                                           pymongo.ASCENDING).limit(10))])

    def tearDown(self):
        self.clearRelatedDb()

    def clearRelatedDb(self):
        edb.get_timeseries_db().remove()
        edb.get_place_db().remove()
        edb.get_stop_db().remove()

        edb.get_trip_new_db().remove()
        edb.get_section_new_db().remove()

    def testTripGeojson(self):
        eaist.segment_current_trips(self.testUUID)
        eaiss.segment_current_sections(self.testUUID)

        tl = esdtl.get_timeline(self.testUUID, 1440658800, 1440745200)
        tl.fill_start_end_places()

        created_trips = tl.trips
        self.assertEquals(len(created_trips), 8)

        trip_geojson = gjfc.trip_to_geojson(created_trips[0], tl)
        logging.debug("trip_geojson = %s" % gj.dumps(trip_geojson, indent=4))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
