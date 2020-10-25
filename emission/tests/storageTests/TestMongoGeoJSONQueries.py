from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest

# Our imports
from emission.core import common
from emission.core.get_database import get_test_db

class TestGeoJSON(unittest.TestCase):
  def setUp(self):
    self.Sections=get_test_db()

  def tearDown(self):
    get_test_db().remove()

  def testGeoWithinPostsData(self):
    post1 = {"author": "Mike",
           "text": "My first blog post!",
           "tags": ["mongodb", "python", "pymongo"],
           'loc':{'type':'Point', 'coordinates':[100,32]}}
    self.Sections.insert_one(post1)
    post2 = {"author": "hh",
           "text": "My 2 blog post!",
           "tags": ["mongodb", "python", "pymongo"],
           "loc":{'type':'Point', 'coordinates':[200,30]}}
    self.Sections.insert_one(post2)

    retVal = []
    for a in self.Sections.find({ "loc" : { "$geoWithin" : { "$polygon" :[ [ 90,31 ],[90,40] ,[ 110,40 ],[110,31]] } } }):
        retVal.append(a)

    self.assertEqual(len(retVal), 1)
    self.assertEqual(retVal[0]['text'], "My first blog post!")
    self.assertEqual(retVal[0]['loc']['coordinates'], [100,32])

  def getTestPolygon(self):
    return [ [ 90.234,-31.0323 ],[95.0343,-45.03453] ,[ 110.02322,-43.3435 ],[100.343423,-33.33423]]

  def testGeoWithOurStructureAndTestPolyFunction(self):
    sec1 = {'track_location':{'type':'Point', 'coordinates':[100,-40]}}
    self.Sections.insert_one(sec1)

    retVal = []
    for a in self.Sections.find({ "track_location" : { "$geoWithin" : { "$polygon" : self.getTestPolygon() } } }):
        retVal.append(a)
    self.assertEqual(len(retVal), 1)

  def getTestPolygon2(self):
    # return [ [30,+50],[30,+100], [60,+100],[60,+50]] # works1
    # return [ [30,+100],[30,+150], [60,+150],[60,+100]] # fails
    # return [ [35,+110],[35,+120], [36,+120],[36,+110]] # fails
    # return [ [30,+50],[30,+100], [60,+100],[60,+50]] # fails
    # return [ [35,+50],[35,+100], [36,+100],[36,+50]] # works2
    # return [ [35,+75],[35,+76], [36,+75],[36,+76]] # works3
    # return [ [35,+85],[35,+86], [36,+85],[36,+86]] # works4
    # return [ [35,+95],[35,+96], [36,+95],[36,+96]] # fails
    # return [ [35,+90],[35,+91], [36,+90],[36,+91]] # FAILS
    # return [ [35,+89],[35,+90], [36,+89],[36,+90]] # WORKS!
    # return [ [89,35],[89,36], [90,35],[90,36]] # works
    return [ [90,35],[90,36], [91,35],[91,36]] # works

  def testGeoWithOurStructureAndTestPolyFunction2(self):
    # pnt = {'type':'Point', 'coordinates': [40, +70]} # works1
    # pnt = {'type':'Point', 'coordinates': [40, +120]} # fails
    # pnt = {'type':'Point', 'coordinates': [35.5, +115]} # fails
    # pnt = {'type':'Point', 'coordinates': [35, +95]} # fails
    # pnt = {'type':'Point', 'coordinates': [35.5, +75]} # works2
    # pnt = {'type':'Point', 'coordinates': [35.5, +75.5]} # works2
    # pnt = {'type':'Point', 'coordinates': [35.5, +85.5]} # works3
    # pnt = {'type':'Point', 'coordinates': [35.5, +85.5]} # works4
    # pnt = {'type':'Point', 'coordinates': [35.5, +95.5]} # fails
    # pnt = {'type':'Point', 'coordinates': [35.5, +90.5]} # fails
    # pnt = {'type':'Point', 'coordinates': [35.5, +89.5]} # WORKS!
    # pnt = {'type':'Point', 'coordinates': [89.5, 35.5]} # works
    pnt = {'type':'Point', 'coordinates': [90.5, 35.5]} # works
    sec1 = {'track_location': pnt}
    self.Sections.insert_one(sec1)

    retVal = []
    for a in self.Sections.find({ "track_location" : { "$geoWithin" : { "$polygon" : self.getTestPolygon2() } } }):
        retVal.append(a)

    self.assertEqual(len(retVal), 1)

  def getTestNegPolygon(self):
    return [ [-90,35],[-90,36],
             [-91,35],[-91,36]] # works
    # return [[-123,36],[-123,38],
    #         [-122,36],[-122,38]]

  def testGeoWithNegativeValues(self):
    test = {'track_location': {'type':'Point', 'coordinates': [-90.5,35.5]} }
    self.Sections.insert_one(test)

    retVal = []
    for a in self.Sections.find({ "track_location" : { "$geoWithin" : { "$polygon" : self.getTestNegPolygon() } } }):
        retVal.append(a)
        print("Found match for %s" % a)

    self.assertEqual(len(retVal), 1)

  def getTestNegPolygon(self):
#     return [[-122,36],[-122,38], # FAILS
#             [-123,36],[-123,38]]
    return [[-122,37],[-122,38],   # WORKS
            [-123,38],[-123,37]]

  def testGeoWithNegativeValues(self):
    test = {'track_location': {'type':'Point', 'coordinates': [-122.5,37.5]} }
    self.Sections.insert_one(test)

    retVal = []
    for a in self.Sections.find({ "track_location" : { "$geoWithin" : { "$polygon" : self.getTestNegPolygon() } } }):
        retVal.append(a)
        print("Found match for %s" % a)

    self.assertEqual(len(retVal), 1)

  def getRealBerkeleyPolygon(self):
    return [[-122,37],[-122,38],
            [-123,38],[-123,37]]

  def testGeoWithOurStructureAndRealPolyFunction(self):
    test = {'track_location': {'type':'Point', 'coordinates': [-122.5, 37.5]} }
    self.Sections.insert_one(test)

    library = {'track_location': {'type':'Point', 'coordinates': [-122.259475, 37.872370]} }
    self.Sections.insert_one(library)

    mclaughlin = {'track_location': {'type':'Point', 'coordinates': [-122.259169, 37.873873]} }
    self.Sections.insert_one(mclaughlin)

    soda = {'track_location': {'type':'Point', 'coordinates': [-122.258740, 37.875711]} }
    self.Sections.insert_one(soda)

    wurster = {'track_location': {'type':'Point', 'coordinates': [-122.254577, 37.870352]} }
    self.Sections.insert_one(wurster)

    retVal = []
    for a in self.Sections.find({ "track_location" : { "$geoWithin" : { "$polygon" : self.getTestNegPolygon() } } }):
        retVal.append(a)
        print("Found match for %s" % a)

    self.assertEqual(len(retVal), 5)

    retVal = []
    for a in self.Sections.find({ "track_location" : { "$geoWithin" : { "$polygon" : self.getRealBerkeleyPolygon() } } }):
        retVal.append(a)
        print("Found match for %s" % a)

    self.assertEqual(len(retVal), 5)

if __name__ == '__main__':
    unittest.main()
