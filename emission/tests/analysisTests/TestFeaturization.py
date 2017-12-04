from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import range
from builtins import *
import unittest
import time
import uuid
import logging

import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.featurization as featurization
import emission.analysis.modelling.tour_model.cluster_pipeline as cp
import emission.storage.timeseries.abstract_timeseries as esta

import emission.tests.analysisTests.tourModelTests.common as etatc
import emission.tests.common as etc

class FeaturizationTests(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(FeaturizationTests, self).__init__(*args, **kwargs)

    def setUp(self):
        self.data = cp.read_data()
        self.testUUID = uuid.uuid4()
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)
        print('there are ' + str(len(self.data)))

    def tearDown(self):
        edb.get_timeseries_db().delete_many({'user_id': self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.testUUID})

    def testCalculatePoints(self):
        feat = featurization.featurization([])
        self.assertTrue(not feat.data)
        feat = featurization.featurization(None)
        self.assertTrue(not feat.data)
        trip = etatc._createTripEntry(self, None, None, None, None)
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
        self.assertTrue(a == b) #defaults to kmeans with invalid clustering method
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
        start = [-122, 47]
        end = [-123,47]
        now = time.time()
        for i in range(10):
            a = etatc._createTripEntry(self, now, now, start, end)
            data.append(a)
        start = [-74, 41]
        end = [-74, 42]
        for i in range(10):
            a = etatc._createTripEntry(self, now, now, start, end)
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
        except Exception as e:
            logging.exception(e.message)
            self.assertTrue(False)

if __name__ == "__main__":
    etc.configLogging()
    unittest.main()
