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
import geojson as gj
import bson.json_util as bju
import os

# Our imports
import emission.core.get_database as edb
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.pipeline_queries as epq
import emission.core.wrapper.motionactivity as ecwm

import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.intake.cleaning.clean_and_resample as eaicr
import emission.analysis.classification.inference.mode.pipeline as eacimp
import emission.analysis.plotting.geojson.geojson_feature_converter as gjfc

import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.stop_queries as esdst
import emission.storage.decorations.section_queries as esds
import emission.storage.decorations.timeline as esdtl
import emission.analysis.intake.cleaning.filter_accuracy as eaicf

# Test imports
import emission.tests.common as etc

class TestGeojsonFeatureConverter(unittest.TestCase):
    def setUp(self):
        self.copied_model_path = etc.copy_dummy_seed_for_inference()
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-27")
        eaicf.filter_accuracy(self.testUUID)

    def tearDown(self):
        self.clearRelatedDb()
        os.remove(self.copied_model_path)

    def clearRelatedDb(self):
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})

    def testTripGeojson(self):
        eaist.segment_current_trips(self.testUUID)
        eaiss.segment_current_sections(self.testUUID)
        eaicl.filter_current_sections(self.testUUID)
        tl = esdtl.get_raw_timeline(self.testUUID, 1440658800, 1440745200)
        self.assertEqual(len(tl.trips), 9)

        eaicr.clean_and_resample(self.testUUID)
        eacimp.predict_mode(self.testUUID)

        tl = esdtl.get_cleaned_timeline(self.testUUID, 1440658800, 1440745200)
        tl.fill_start_end_places()

        created_trips = tl.trips
        self.assertEqual(len(created_trips), 9)

        trip_geojson = gjfc.trip_to_geojson(created_trips[0], tl)
        logging.debug("first trip_geojson = %s" % bju.dumps(trip_geojson, indent=4))

        self.assertEqual(trip_geojson.type, "FeatureCollection")
        self.assertEqual(trip_geojson.properties["feature_type"], "trip")
        self.assertEqual(len(trip_geojson.features), 5)

        day_geojson = gjfc.get_geojson_for_timeline(self.testUUID, tl)
        self.assertEqual(len(day_geojson), 8)
        self.assertEqual(day_geojson[-1].type, "FeatureCollection")
        self.assertEqual(day_geojson[-1].properties["feature_type"], "trip")
        self.assertEqual(len(day_geojson[-1].features), 5)


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
