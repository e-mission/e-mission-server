import unittest
import modeinfer.featurecalc as fc

class TestFeatureCalc(unittest.TestCase):
  def testCalSpeedWithZeroTime(self):
    trackpoint1 = {"track_location": {"coordinates": [-122.0861645, 37.3910201]},
                   "time" : "20150127T203305-0800"}
    trackpoint2 = {"track_location": {"coordinates": [-122.0858963, 37.3933358]},
                   "time" : "20150127T203305-0800"}
    self.assertEqual(fc.calSpeed(trackpoint1, trackpoint2), None)

  def testCalSpeedsWithZeroTime(self):
    trackpoint1 = {"track_location": {"coordinates": [-122.0861645, 37.3910201]},
                   "time" : "20150127T203305-0800"}
    trackpoint2 = {"track_location": {"coordinates": [-122.0858963, 37.3933358]},
                   "time" : "20150127T203305-0800"}
    testSeg = {"track_points": [trackpoint1, trackpoint2]}
    self.assertEqual(fc.calSpeeds(testSeg), [0])

  def testCalSpeedsWithOnePoint(self):
    trackpoint1 = {"track_location": {"coordinates": [-122.0861645, 37.3910201]},
                   "time" : "20150127T203305-0800"}
    testSeg = {"track_points": [trackpoint1]}
    self.assertEqual(len(fc.calSpeeds(testSeg)), 0)

  def testCalAccelsWithOnePoint(self):
    trackpoint1 = {"track_location": {"coordinates": [-122.0861645, 37.3910201]},
                   "time" : "20150127T203305-0800"}
    testSeg = {"track_points": [trackpoint1]}
    self.assertEqual(fc.calAccels(testSeg), None)

  def testCalSpeedsWithOnePoint(self):
    trackpoint1 = {"track_location": {"coordinates": [-122.0861645, 37.3910201]},
                   "time" : "20150127T203305-0800"}
    testSeg = {"track_points": [trackpoint1]}
    self.assertEqual(len(fc.calSpeeds(testSeg)), 0)

if __name__ == '__main__':
    unittest.main()
