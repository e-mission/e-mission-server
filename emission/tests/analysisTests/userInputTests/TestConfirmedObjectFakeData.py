import unittest
import logging
import json
import bson.json_util as bju
import argparse
import numpy as np
import uuid

import emission.core.wrapper.entry as ecwe
import emission.analysis.userinput.matcher as eaum
import emission.storage.decorations.trip_queries as esdt
import emission.storage.timeseries.abstract_timeseries as esta

import emission.core.wrapper.motionactivity as ecwma
import emission.core.wrapper.modeprediction as ecwmp

import emission.analysis.userinput.matcher as eaum

# Test imports
import emission.tests.common as etc

class TestConfirmedObjectFakeData(unittest.TestCase):

    def setUp(self):
        self.testUUID = uuid.uuid4()
        self.test_ts = esta.TimeSeries.get_time_series(self.testUUID)
        self.BLANK_RESULT={"distance": {}, "duration": {}, "count": {}}

    def tearDown(self):
        logging.debug("Clearing related databases for %s" % self.testUUID)
        self.clearRelatedDb()
        logging.info("tearDown complete")


    def clearRelatedDb(self):
        import emission.core.get_database as edb
        logging.info("Timeseries delete result %s" % edb.get_timeseries_db().delete_many({"user_id": self.testUUID}).raw_result)
        logging.info("Analysis delete result %s" % edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID}).raw_result)
        logging.info("Usercache delete result %s" % edb.get_usercache_db().delete_many({"user_id": self.testUUID}).raw_result)

    def testGetSectionSummaryCleaned(self):
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/cleaned_trip"}, "data": {}})
        fake_ct_id = self.test_ts.insert(fake_ct)
        fake_cs_list = [
            ecwe.Entry({"metadata": {"key": "analysis/cleaned_section", "write_ts": 1},
                "data": {"distance": 500, "duration": 50, "sensed_mode": ecwma.MotionTypes['BICYCLING'].value,
                "trip_id": fake_ct["_id"]}}),
            ecwe.Entry({"metadata": {"key": "analysis/cleaned_section", "write_ts": 2},
                "data": {"distance": 200, "duration": 20, "sensed_mode": ecwma.MotionTypes['BICYCLING'].value,
                "trip_id": fake_ct["_id"]}}),
            ecwe.Entry({"metadata": {"key": "analysis/cleaned_section", "write_ts": 3},
                "data": {"distance": 200, "duration": 20, "sensed_mode": ecwma.MotionTypes['UNKNOWN'].value,
                "trip_id": fake_ct["_id"]}}),
            ecwe.Entry({"metadata": {"key": "analysis/cleaned_section", "write_ts": 4},
                "data": {"distance": 400, "duration": 40, "sensed_mode": ecwma.MotionTypes['IN_VEHICLE'].value,
                "trip_id": fake_ct["_id"]}}),
            ecwe.Entry({"metadata": {"key": "analysis/cleaned_section", "write_ts": 5},
                "data": {"distance": 300, "duration": 30, "sensed_mode": ecwma.MotionTypes['WALKING'].value,
                "trip_id": fake_ct["_id"]}})]
     
        fake_cs_ids = [self.test_ts.insert(fake_cs) for fake_cs in fake_cs_list]

        cleaned_section_summary = eaum.get_section_summary(self.test_ts, fake_ct, "analysis/cleaned_section")
        logging.debug(cleaned_section_summary)
        self.assertEqual(cleaned_section_summary['distance'],
            {'BICYCLING': 700, 'UNKNOWN': 200, 'IN_VEHICLE': 400, 'WALKING': 300})
        self.assertEqual(cleaned_section_summary['duration'],
            {'BICYCLING': 70, 'UNKNOWN': 20, 'IN_VEHICLE': 40, 'WALKING': 30})
        self.assertEqual(cleaned_section_summary['count'],
            {'BICYCLING': 2, 'UNKNOWN': 1, 'IN_VEHICLE': 1, 'WALKING': 1})

    def testGetSectionSummaryInferred(self):
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/cleaned_trip"}, "data": {}})
        fake_ct_id = self.test_ts.insert(fake_ct)
        fake_cs_list = [
            ecwe.Entry({"metadata": {"key": "analysis/inferred_section", "write_ts": 1},
                "data": {"distance": 500, "duration": 50, "sensed_mode": ecwmp.PredictedModeTypes['BUS'].value,
                "trip_id": fake_ct["_id"]}}),
            ecwe.Entry({"metadata": {"key": "analysis/inferred_section", "write_ts": 2},
                "data": {"distance": 200, "duration": 20, "sensed_mode": ecwmp.PredictedModeTypes['BUS'].value,
                "trip_id": fake_ct["_id"]}}),
            ecwe.Entry({"metadata": {"key": "analysis/inferred_section", "write_ts": 2},
                "data": {"distance": 200, "duration": 20, "sensed_mode": ecwmp.PredictedModeTypes['TRAIN'].value,
                "trip_id": fake_ct["_id"]}}),
            ecwe.Entry({"metadata": {"key": "analysis/inferred_section", "write_ts": 3},
                "data": {"distance": 300, "duration": 30, "sensed_mode": ecwmp.PredictedModeTypes['CAR'].value,
                "trip_id": fake_ct["_id"]}})]
     
        fake_cs_ids = [self.test_ts.insert(fake_cs) for fake_cs in fake_cs_list]

        inferred_section_summary = eaum.get_section_summary(self.test_ts, fake_ct, "analysis/inferred_section")
        logging.debug(inferred_section_summary)
        self.assertEqual(inferred_section_summary['distance'],
            {'BUS': 700, 'TRAIN': 200, 'CAR': 300})
        self.assertEqual(inferred_section_summary['duration'],
            {'BUS': 70, 'TRAIN': 20, 'CAR': 30})
        self.assertEqual(inferred_section_summary['count'],
            {'BUS': 2, 'TRAIN': 1, 'CAR': 1})

    def testNoSections(self):
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/cleaned_trip"}, "data": {}})
        fake_ct_id = self.test_ts.insert(fake_ct)
        self.assertEqual(eaum.get_section_summary(self.test_ts, fake_ct, "analysis/inferred_section"),
            self.BLANK_RESULT)
        self.assertEqual(eaum.get_section_summary(self.test_ts, fake_ct, "analysis/cleaned_section"),
            self.BLANK_RESULT)

    def testInvalidInput(self):
        with self.assertRaises(TypeError) as te:
            eaum.get_section_summary(self.test_ts, None, "foobar")
        self.assertEqual(str(te.exception), "'NoneType' object is not subscriptable")

        with self.assertRaises(KeyError) as ke:
            fake_ct = ecwe.Entry({"metadata": {"key": "analysis/cleaned_trip"}, "data": {}})
            eaum.get_section_summary(self.test_ts, fake_ct, "foobar")
        self.assertEqual(str(ke.exception), "'user_id'")

        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/cleaned_trip"}, "data": {}})
        fake_ct_id = self.test_ts.insert(fake_ct)
        self.assertEqual(eaum.get_section_summary(self.test_ts, fake_ct, "foobar"), self.BLANK_RESULT)

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
