# Standard imports
import unittest
import datetime as pydt
import logging
import json
import geojson as gj

# Our imports
import emission.core.get_database as edb
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.pipeline_queries as epq
import emission.core.wrapper.motionactivity as ecwm

import emission.analysis.plotting.geojson.geojson_feature_converter as gjfc
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.intake.cleaning.clean_and_resample as eaicr

import emission.analysis.intake.segmentation.trip_segmentation as eaist

import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.stop_queries as esdst
import emission.storage.decorations.section_queries as esds
import emission.storage.decorations.timeline as esdtl

# Test imports
import emission.tests.common as etc

class TestGeojsonFeatureConverter(unittest.TestCase):
    def setUp(self):
        self.clearRelatedDb()
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-27")

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
        eaicl.filter_current_sections(self.testUUID)
        eaicr.clean_and_resample(self.testUUID)

        tl = esdtl.get_cleaned_timeline(self.testUUID, 1440658800, 1440745200)
        tl.fill_start_end_places()

        created_trips = tl.trips
        self.assertEquals(len(created_trips), 8)

        trip_geojson = gjfc.trip_to_geojson(created_trips[0], tl)
        logging.debug("trip_geojson = %s" % gj.dumps(trip_geojson, indent=4))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
