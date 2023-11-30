import unittest
import json
from featurecalc import calDistance, calSpeed, calHeading, calAvgSpeed, calSpeeds, calAccels, getIthMaxSpeed, getIthMaxAccel
import pygeocoder


class TestFeatureCalc(unittest.TestCase):
  # All the test data is obtained by using 
  # Sections.find_one({"$and": [{'type': 'move'}, {'confirmed_mode': <modeId>}]})
  # and then picking the first two points from the track points for that section
  def setUp(self):
    self.walktp1 = {"track_location": {"type": "Point",
        "coordinates": [37.8724267522, -122.2593326013]}, "time": "20140415T182218Z"}
    self.walktp2 = {"track_location": {"type": "Point",
        "coordinates": [37.8722939116, -122.2594439528]}, "time": "20140415T182251Z"}

    self.biketp1 = {"track_location": {"type": "Point", "coordinates": [37.8385561216, -122.2495945853]}, "time": "20140418T181035-0700"}
    self.biketp2 = {"track_location": {"type": "Point", "coordinates": [37.838834329, -122.249646471]}, "time": "20140418T181100-0700"}

    self.traintp1 = {"track_location": {"type": "Point", "coordinates": [37.8419243845, -122.251608766]}, "time": "20140418T083731-0700"}
    self.traintp2 = {"track_location": {"type": "Point", "coordinates": [37.841983358, -122.2516275124]}, "time": "20140418T083748-0700"}

    self.cartp1 = {"track_location": {"type": "Point", "coordinates": [37.8796206126, -122.272393763]}, "time": "20140418T013558Z"}
    self.cartp2 = {"track_location": {"type": "Point", "coordinates": [37.8796948352, -122.2724807525]}, "time": "20140418T013618Z"}

    from datetime import datetime

    segments = json.load(open("testFeatureCalcData.json"))
    self.walkSegment = segments[0]
    self.walkSegment['section_start_datetime'] = datetime(2014, 4, 15, 18, 22, 18)
    self.walkSegment['section_end_datetime'] = datetime(2014, 4, 15, 18, 31, 27)

    self.bikeSegment = segments[1]
    self.bikeSegment['section_start_datetime'] = datetime(2014, 4, 19, 1, 10, 35)
    self.bikeSegment['section_end_datetime'] = datetime(2014, 4, 19, 1, 23, 16)

    self.trainSegment = segments[2]
    self.trainSegment['section_start_datetime'] = datetime(2014, 4, 18, 15, 37, 31)
    self.trainSegment['section_end_datetime'] = datetime(2014, 4, 18, 15, 48, 3)

    self.carSegment = segments[3]
    self.carSegment['section_start_datetime'] = datetime(2014, 4, 18, 1, 35, 58)
    self.carSegment['section_end_datetime'] = datetime(2014, 4, 18, 1, 42, 46)

  # We spot check by using real values from the test data and comparing them to
  # the calculations at http://www.movable-type.co.uk/scripts/latlong.html
  def testCalDistance(self):
    self.assertAlmostEqual(
        calDistance([37.8724267522, -122.2593326013], [37.8678553385, -122.2597410423]),
          509.6, places=1)

    self.assertAlmostEqual(
      calDistance(self.walktp1['track_location']['coordinates'], self.walktp2['track_location']['coordinates']),
      17.71, places=1)

    # points obtained from a bike trip
    self.assertAlmostEqual(
      calDistance(self.biketp1['track_location']['coordinates'], self.biketp2['track_location']['coordinates']),
      31.27, places=1)

    self.assertAlmostEqual(
      calDistance(self.traintp1['track_location']['coordinates'], self.traintp2['track_location']['coordinates']),
      6.761, places=1)

    self.assertAlmostEqual(
      calDistance(self.cartp1['track_location']['coordinates'], self.cartp2['track_location']['coordinates']),
      11.24, places=1)
    
  def testCalcSpeed(self):
    # points obtained from a walk trip
    self.assertAlmostEqual(
      calSpeed(self.walktp1, self.walktp2), 0.53666, places=2)

    self.assertAlmostEqual(
      calSpeed(self.biketp1, self.biketp2), 1.25, places=2)

    self.assertAlmostEqual(
      calSpeed(self.traintp1, self.traintp2), 0.3977, places=2)

    self.assertAlmostEqual(
      calSpeed(self.cartp1, self.cartp2), 0.562, places=2)

  def testCalcHeading(self):
    # points from a walking trip
    self.assertAlmostEqual(
      calHeading(self.walktp1['track_location']['coordinates'], self.walktp2['track_location']['coordinates']),
      -147, places=0)

    self.assertAlmostEqual(
      calHeading(self.biketp1['track_location']['coordinates'], self.biketp2['track_location']['coordinates']),
      -8.37, places=0)

    self.assertAlmostEqual(
      calHeading(self.traintp1['track_location']['coordinates'], self.traintp2['track_location']['coordinates']),
      -14.09, places=0)

    self.assertAlmostEqual(
      calHeading(self.cartp1['track_location']['coordinates'], self.cartp2['track_location']['coordinates']),
      -43, places=0)

    # The website returns only a positive heading - it converts a negative heading to positive
    # by subtracting from 360. I think we can deal with negative headings, so we don't subtract
    # but then we need to fix the value that we compare against
    # 184deg 02'04''
    self.assertAlmostEqual(
        calHeading([37.8724267522, -122.2593326013], [37.8678553385, -122.2597410423]),
          - (360 - 184.035), places=1)

  def testAvgSpeeds(self):
    self.assertAlmostEqual(
      calAvgSpeed(self.walkSegment), 1.14025, places = 2)

    self.assertAlmostEqual(
      calAvgSpeed(self.bikeSegment), 4.92509, places = 2)
    
    self.assertAlmostEqual(
      calAvgSpeed(self.trainSegment), 4.36708, places = 2)

    self.assertAlmostEqual(
      calAvgSpeed(self.carSegment), 4.52696, places = 2)

  def testSegSpeeds(self):
    self.assertAlmostEqual(
      calSpeeds(self.walkSegment)[0], 0.53666, places=2)

    self.assertAlmostEqual(
      calSpeeds(self.bikeSegment)[0], 1.25, places=2)

    self.assertAlmostEqual(
      calSpeeds(self.trainSegment)[0], 0.3977, places=2)

    self.assertAlmostEqual(
      calSpeeds(self.carSegment)[0], 0.562, places=2)

  def testSegSecondSpeeds(self):
    self.assertAlmostEqual(
      calSpeeds(self.walkSegment)[1], 0.47711, places=2) # 52 secs

    self.assertAlmostEqual(
      calSpeeds(self.bikeSegment)[1], 2.05027, places=2) # 181 secs

    self.assertAlmostEqual(
      calSpeeds(self.trainSegment)[1], 5.61904, places=2) # 21 secs

    self.assertAlmostEqual(
      calSpeeds(self.carSegment)[1], 0.26278, places=2) # 19 secs

  def testFirstAccel(self):
    self.assertAlmostEqual(
      calAccels(self.walkSegment)[0], 0.01626, places=3) # 33 secs

    self.assertAlmostEqual(
      calAccels(self.bikeSegment)[0], 0.05, places=4) # 25 secs

    self.assertAlmostEqual(
      calAccels(self.trainSegment)[0], 0.02339, places=4) # 17 secs

    self.assertAlmostEqual(
      calAccels(self.carSegment)[0], 0.02810, places=4) # 20 secs

  def testSecondAccel(self):
    self.assertAlmostEqual(
      calAccels(self.walkSegment)[1], -0.00114, places=4) # 52 secs

    self.assertAlmostEqual(
      calAccels(self.bikeSegment)[1], 0.00442, places=4) # 181 secs

    self.assertAlmostEqual(
      calAccels(self.trainSegment)[1], 0.24863, places=3) # 21 secs

    self.assertAlmostEqual(
      calAccels(self.carSegment)[1], -0.01574, places=4) # 19 secs


if __name__ == '__main__':
    unittest.main()
