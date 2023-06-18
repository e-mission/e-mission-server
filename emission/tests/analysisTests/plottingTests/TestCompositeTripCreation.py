import unittest
import datetime as pydt
import logging
import json
import geojson as gj
import os
import uuid
import numpy as np

# Our imports
import emission.core.get_database as edb
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.pipeline_queries as epq
import emission.core.wrapper.motionactivity as ecwm
import emission.core.wrapper.entry as ecwe

import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.intake.cleaning.clean_and_resample as eaicr
import emission.analysis.classification.inference.mode.pipeline as eacimp
import emission.analysis.plotting.geojson.geojson_feature_converter as gjfc
import emission.analysis.userinput.matcher as eaum

import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.stop_queries as esdst
import emission.storage.decorations.section_queries as esds
import emission.storage.decorations.timeline as esdtl
import emission.analysis.intake.cleaning.filter_accuracy as eaicf

# Test imports
import emission.tests.common as etc

class TestCompositeTripCreation(unittest.TestCase):
    def setUp(self):
        # Thanks to M&J for the number!
        np.random.seed(61297777)
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2016-08-04")
        etc.runIntakePipeline(self.testUUID)
        self.testTs = esta.TimeSeries.get_time_series(self.testUUID)

    def tearDown(self):
        self.clearRelatedDb()

    def clearRelatedDb(self):
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})

    def testUpdateConfirmedAndComposite(self):
        # read confirmed trips
        confirmedTrips = list(self.testTs.find_entries(["analysis/confirmed_trip"]))
        self.assertEqual(len(confirmedTrips), 9)

        # read composite trips
        compositeTrips = self.testTs.get_data_df("analysis/composite_trip")
        self.assertEqual(len(compositeTrips), 9)
        
        # check that it doesn't have start_ts and confidence threshold
        self.assertEqual(compositeTrips["additions"].apply(lambda a: len(a)).to_list(), [0] * 9)
        self.assertEqual(compositeTrips["user_input"].apply(lambda a: len(a)).to_list(), [0] * 9)
        self.assertEqual(compositeTrips["end_confirmed_place"].apply(lambda a: len(a["data"]["additions"])).to_list(), [0] * 9)

#         self.assertEqual(list(compositeTrips["start_ts"]), [1470341031.235, 1470343292.1363852,
#             1470352238.7396843, 1470354036.959, 1470354386.5618007, 1470355612.592, 1470356315.8404644,
#             1470357578.288795, 1470364485.744782])

        # These are actually separate tests, but we call them from this main test so that we
        # can avoid re-running the pipeline multiple times
        self._testUpdateConfirmedTripProperties()
        self._testSetConfirmedTripAdditions()
        self._testSetConfirmedTripUserInput()
           

    def _testUpdateConfirmedTripProperties(self):
        # read confirmed trips
        confirmedTrips = list(self.testTs.find_entries(["analysis/confirmed_trip"]))
        self.assertEqual(len(confirmedTrips), 9)

        # set a couple of confirmed trip properties
        tripSacrifice = confirmedTrips[0]
        tripSacrifice["data"]["start_ts"] = 1000
        tripSacrifice["data"]["start_fmt_time"] = "I want to go back in time"
        eaum.update_confirmed_and_composite(ecwe.Entry(tripSacrifice))
        
        compositeTrips = list(self.testTs.find_entries(["analysis/composite_trip"]))
        tripExpected = compositeTrips[0]

        self.assertEqual(tripExpected["data"]["start_ts"], 1000)
        self.assertEqual(tripExpected["data"]["start_fmt_time"], "I want to go back in time")
        print("testUpdateConfirmedTripProperties DONE")


    def _testSetConfirmedTripAdditions(self):
        # read confirmed trips
        confirmedTrips = list(self.testTs.find_entries(["analysis/confirmed_trip"]))
        self.assertEqual(len(confirmedTrips), 9)

        # set a couple of confirmed trip properties
        tripSacrifice = confirmedTrips[1]
        ADDITIONS = ["mimi", "fifi", "gigi", "bibi"]
        tripSacrifice["data"]["additions"] = ADDITIONS
        eaum.update_confirmed_and_composite(ecwe.Entry(tripSacrifice))
        
        compositeTrips = list(self.testTs.find_entries(["analysis/composite_trip"]))
        tripExpected = compositeTrips[1]

        self.assertEqual(tripExpected["data"]["additions"], ADDITIONS)
        print("testSetConfirmedTripAdditions DONE")

    def _testSetConfirmedTripUserInput(self):
        # read confirmed trips
        confirmedTrips = list(self.testTs.find_entries(["analysis/confirmed_trip"]))
        self.assertEqual(len(confirmedTrips), 9)

        # set a couple of confirmed trip properties
        tripSacrifice = confirmedTrips[2]
        USERINPUT = {"mimi": 1, "fifi": 100, "gigi": 200, "bibi": 300}
        tripSacrifice["data"]["user_input"] = USERINPUT
        eaum.update_confirmed_and_composite(ecwe.Entry(tripSacrifice))
        
        compositeTrips = list(self.testTs.find_entries(["analysis/composite_trip"]))
        tripExpected = compositeTrips[2]

        self.assertEqual(tripExpected["data"]["user_input"], USERINPUT)
        print("testSetConfirmedTripUserInput DONE")
        
    def _testUpdateConfirmedPlaceProperties(self):
        # read confirmed places
        confirmedPlaces = list(self.testTs.find_entries(["analysis/confirmed_place"]))
        self.assertEqual(len(confirmedPlaces), 10)

        # set a couple of confirmed place properties
        # this has to be a later place because the zeroth place is not the end place for any trip
        placeSacrifice = confirmedPlaces[2]
        placeSacrifice["data"]["exit_ts"] = 1000
        placeSacrifice["data"]["exit_fmt_time"] = "I want to go back in time"
        eaum.update_confirmed_and_composite(ecwe.Entry(placeSacrifice))
        
        compositeTrips = list(self.testTs.find_entries(["analysis/composite_trip"]))
        tripExpected = compositeTrips[1]

        self.assertEqual(tripExpected["data"]["end_composite_place"]["data"]["exit_ts"], 1000)
        self.assertEqual(tripExpected["data"]["end_composite_place"]["data"]["exit_fmt_time"], "I want to go back in time")
        print("testUpdateConfirmedPlaceProperties DONE")

    def _testSetConfirmedPlaceAdditions(self):
        # read confirmed places
        confirmedPlaces = list(self.testTs.find_entries(["analysis/confirmed_place"]))
        self.assertEqual(len(confirmedPlaces), 10)

        # set a couple of confirmed place properties
        # this has to be a later place because the zeroth place is not the end place for any trip
        placeSacrifice = confirmedPlaces[5]
        ADDITIONS = ["mimi", "fifi", "gigi", "bibi"]
        placeSacrifice["data"]["additions"] = ADDITIONS
        eaum.update_confirmed_and_composite(ecwe.Entry(placeSacrifice))
        
        compositeTrips= list(self.testTs.find_entries(["analysis/composite_trip"]))
        tripExpected = compositeTrips[4]

        self.assertEqual(tripExpected["data"]["end_composite_place"]["data"]["additions"], ADDITIONS)
        print("testUpdateConfirmedPlaceAdditions DONE")



if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
