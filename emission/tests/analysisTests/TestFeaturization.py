import unittest
import emission.core.get_database as edb
import sys
import emission.analysis.modelling.tour_model.featurization as featurization
import emission.simulation.trip_gen as tg

class FeaturizationTests(unittest.TestCase):
    def setUp(self):
        #tg.create_fake_trips()
        
        db = edb.get_fake_trips_db()
        
        trips = db.find()
        self.data = list(trips)

    def tearDown(self):
        return

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
        feat = featurization.featurization(self.data)
        self.assertTrue(len(feat.points) == len(feat.data))
        for p in feat.points:
            self.assertTrue(None not in p)

    def testCluster(self):
        feat = featurization.featurization(self.data)
        try: 
            feat.cluster(min_clusters=2, max_clusters=10)
        except Exception:
            self.assertTrue(False)

        self.assertTrue(len(feat.labels) == len(feat.points))
        self.assertTrue(feat.clusters == len(set(feat.labels)))

        a = feat.cluster(name='kmeans', min_clusters=5, max_clusters=20)
        self.assertTrue(len(feat.labels) == len(feat.points))
        self.assertTrue(feat.clusters == len(set(feat.labels)))
        #feat.cluster(name='kmedoids', min_clusters=5, max_clusters=20)
        self.assertTrue(len(feat.labels) == len(feat.points))
        self.assertTrue(feat.clusters == len(set(feat.labels)))
        b = feat.cluster(name='nonname', min_clusters=5, max_clusters=20)
        self.assertTrue(a == b)
        feat.cluster(min_clusters=len(self.data)+1)
        #self.assertTrue(feat.clusters == len(set(feat.labels)))
        c = feat.cluster(min_clusters = 0, max_clusters=20)
        d = feat.cluster(min_clusters = 2, max_clusters=20)
        self.assertTrue(c == d)

        try:
            feat.cluster(min_clusters = 10, max_clusters=2)
        except ValueError:
            self.assertTrue(True)
        except Exception:
            self.assertTrue(False)

    def testCheckClusters(self):
        feat = featurization.featurization(self.data)
        a = feat.check_clusters()
        self.assertTrue(a == None)
        feat.cluster(min_clusters=2, max_clusters=10)
        try:
            feat.check_clusters()
        except Exception:
            self.assertTrue(False)

    def testMapIndividuals(self):
        feat = featurization.featurization(self.data)
        a = feat.map_individuals()
        self.assertTrue(a == None)
        feat.cluster(min_clusters=2, max_clusters=10)
        b = feat.map_individuals()
        self.assertTrue(b == None)
        
    def testMapClusters(self):
        feat = featurization.featurization(self.data)
        a = feat.map_clusters()
        self.assertTrue(a ==None)
        feat.cluster(min_clusters=2, max_clusters=10)
        try:
            feat.map_clusters()
        except Exception:
            self.assertTrue(False)

if __name__ == "__main__":
    unittest.main()
