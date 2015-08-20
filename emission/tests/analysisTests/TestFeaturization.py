import unittest
import emission.core.get_database as edb
import sys
import emission.analysis.modelling.tour_model.featurization as featurization
from emission.core.wrapper.trip import Trip, Coordinate
import emission.analysis.modelling.tour_model.cluster_pipeline as cp
import datetime
import os, os.path

class FeaturizationTests(unittest.TestCase):
    def setUp(self):
        self.data = cp.read_data()
    def tearDown(self):
        pass

    def testCalculatePoints(self):
        try:
            data = []
            featurization.featurization(data)
        except ValueError:
            self.assertTrue(True)
        except Exception:
            self.assertTrue(False)
        try:
            data = None
            featurization.featurization(data)
        except ValueError:
                self.assertTrue(True)
        except Exception:
                self.assertTrue(False)
        trip = Trip(None, None, None, None, None, None, None, None)
        data = [trip]
        try:
            feat = featurization.featurization(data)
        except AttributeError:
            self.assertTrue(True)
        except Exception:
            self.assertTrue(False)
        feat = featurization.featurization(self.data)
        self.assertTrue(len(feat.points) == len(feat.data))
        for p in feat.points:
            self.assertTrue(None not in p)

    def testCluster(self):
        feat = featurization.featurization(self.data)
        feat.cluster(min_clusters=2, max_clusters=10)
        self.assertTrue(len(feat.labels) == len(feat.points))
        self.assertTrue(feat.clusters == len(set(feat.labels)))
        a = feat.cluster(name='kmeans', min_clusters=5, max_clusters=20)
        self.assertTrue(len(feat.labels) == len(feat.points))
        self.assertTrue(feat.clusters == len(set(feat.labels)))
        b = feat.cluster(name='nonname', min_clusters=5, max_clusters=20)
        self.assertTrue(a == b)
        feat.cluster(min_clusters=len(self.data)+1)
        c = feat.cluster(min_clusters = 0, max_clusters=20)
        d = feat.cluster(min_clusters = 2, max_clusters=20)
        self.assertTrue(c == d)
        try:
            feat.cluster(min_clusters = 10, max_clusters=2)
        except ValueError:
            self.assertTrue(True)
        except Exception:
            self.assertTrue(False)

        data = []
        start = Coordinate(47,-122)
        end = Coordinate(47,-123)
        for i in range(10):
            now = datetime.datetime.now()
            a = Trip(None, None, None, None, now, now, start, end)
            data.append(a)
        start = Coordinate(41,-74)
        end = Coordinate(42, -74)
        for i in range(10):
            now = datetime.datetime.now()
            a = Trip(None, None, None, None, now, now, start, end)
            data.append(a)
        feat = featurization.featurization(data)
        feat.cluster()
        self.assertTrue(len(set(feat.labels)) == 2)

    def testCheckClusters(self):
        feat = featurization.featurization(self.data)
        a = feat.check_clusters()
        self.assertTrue(a == None)
        feat.cluster(min_clusters=2, max_clusters=10)
        try:
            feat.check_clusters()
        except Exception:
            self.assertTrue(False)

    def testMapClusters(self):
        if os.path.isfile('./mymap.html'):
            os.remove('./mymap.html')
        if os.path.isfile('./mylabels.html'):
            os.remove('./mylabels.html')
        feat = featurization.featurization(self.data)
        feat.map_clusters()
        feat.cluster(min_clusters=2, max_clusters=10)
        feat.map_clusters()
        self.assertTrue(not os.path.isfile('./mymap.html'))
        self.assertTrue(os.path.isfile('./mylabels.html'))
        os.remove('./mylabels.html')

if __name__ == "__main__":
    unittest.main()
