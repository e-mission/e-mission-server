from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import copy
import numpy as np

import emission.analysis.section_features as fc

import emission.core.wrapper.cleanedsection as ecwcs
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.modeprediction as ecwm
import emission.core.wrapper.motionactivity as ecwma
import emission.storage.decorations.analysis_timeseries_queries as esda

import emission.tests.common as etc

class TestFeatureCalc(unittest.TestCase):
  def setUp(self):
    self.point1 = {
        "data": {
            "loc": {
                "type": "Point",
                "coordinates": [
                    -46.5468285,
                    -23.5538387
                ]
            },
            "fmt_time": "2017-07-31T21:03:06-03:00",
            "altitude": 682,
            "ts": 1501545786,
            "longitude": -46.5468285,
            "filter": "time",
            "elapsedRealtimeNanos": 2520735980294548,
            "latitude": -23.5538387,
            "heading": 0,
            "sensed_speed": 0,
            "accuracy": 45
        },
        "metadata": {
            "write_fmt_time": "2017-07-31T21:03:06.319000-03:00",
            "write_ts": 1501545786.319,
            "time_zone": "America/Sao_Paulo",
            "key": "background/filtered_location",
            "type": "sensor-data"
        }
    }
    self.point2 = copy.copy(self.point1)
    self.point2["data"]["latitude"] = self.point1["data"]["latitude"]+0.001
    self.point2["data"]["longitude"] = self.point1["data"]["longitude"]+0.001
    self.point2["data"]["loc"]["coordinates"] = [self.point2["data"]["longitude"], self.point2["data"]["latitude"]]

  def testCalSpeedWithZeroTime(self):
    self.assertEqual(fc.calSpeed(self.point1, self.point2), None)

  def testCalSpeedsWithZeroTime(self):
    testSec = ecwcs.Cleanedsection()
    testSec.speeds = []
    self.assertEqual(fc.calSpeeds(testSec), [])

  def testCalSpeedsWithOnePoint(self):
    testSec = ecwcs.Cleanedsection()
    testSec.speeds = []
    self.assertEqual(fc.calSpeeds(testSec), [])

  def testCalSpeedsWithTwoPoints(self):
    testSec = ecwcs.Cleanedsection()
    testSec.speeds = [5]
    self.assertEqual(fc.calSpeeds(testSec), [5])

  def testCalAccelsWithOnePoint(self):
    testSec = ecwcs.Cleanedsection()
    testSec.speeds = []
    self.assertEqual(fc.calAccels(testSec), None)

  def testCalAccelsWithTwoPoints(self):
    testSec = ecwcs.Cleanedsection()
    testSec.speeds = [3]
    self.assertEqual(fc.calAccels(testSec).tolist(), [0.1])

  def testCalAccelsWithThreePoints(self):
    testSec = ecwcs.Cleanedsection()
    testSec.speeds = [3, 6]
    print(fc.calAccels(testSec))
    self.assertEqual(fc.calAccels(testSec).tolist(), [3/30, 3/30])

  def testSelectInferredModeImbalanced(self):
    # dummy_inferred_mode = dim
    dim = ecwm.Modeprediction()
    dim.trip_id = "test_trip_id"
    dim.section_id = "test_section_id"
    dim.algorithm_id = ecwm.AlgorithmTypes.SEED_RANDOM_FOREST
    dim.sensed_mode = ecwma.MotionTypes.WALKING
    dim.predicted_mode_map = {"WALKING": 0.9, "BUS": 0.1}
    dim.start_ts = 0
    dim.end_ts = 0

    self.assertEqual(fc.select_inferred_mode([dim]), 'WALKING')

  def testSelectInferredModeEqual(self):
    # dummy_inferred_mode = dim
    # select the lower value mode, typically = lower carbon intensity mode
    dim = ecwm.Modeprediction()
    dim.trip_id = "test_trip_id"
    dim.section_id = "test_section_id"
    dim.algorithm_id = ecwm.AlgorithmTypes.SEED_RANDOM_FOREST
    dim.sensed_mode = ecwma.MotionTypes.WALKING
    dim.predicted_mode_map = {"WALKING": 0.5, "BUS": 0.5}
    dim.start_ts = 0
    dim.end_ts = 0

    self.assertEqual(fc.select_inferred_mode([dim]), 'WALKING')

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
