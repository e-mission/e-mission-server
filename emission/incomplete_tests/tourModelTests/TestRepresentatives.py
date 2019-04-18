from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import next
from builtins import range
from builtins import *
from past.utils import old_div
import unittest
import time
import logging

import emission.analysis.modelling.tour_model.representatives as rep
import emission.analysis.modelling.tour_model.featurization as feat

import emission.tests.analysisTests.tourModelTests.common as etatc

import emission.tests.common as etc

class RepresentativesTests(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(RepresentativesTests, self).__init__(*args, **kwargs)

    def setUp(self):
        etatc._setup(self)
        n = old_div(len(self.data),5)
        self.labels = feat.featurization(self.data).cluster(min_clusters=n, max_clusters=n)

    def tearDown(self):
        etatc._tearDown(self)

    def testInit(self):
        repy = rep.representatives(None, None)
        self.assertTrue(not repy.data)
        self.assertTrue(not repy.labels)
        if self.labels:
            try:
                a = self.labels.pop()
                rep.representatives(self.data, self.labels)
            except ValueError:
                self.assertTrue(True)
                self.labels.append(a)
            except Exception:
                self.assertTrue(False)
        repy = rep.representatives(self.data, self.labels)

    def testListClusters(self):
        repy = rep.representatives(self.data, self.labels)
        repy.list_clusters()
        sum = 0
        for i in range(len(repy.clusters)):
            self.assertTrue(len(repy.clusters[i]) == self.labels.count(i))
            sum += len(repy.clusters[i])
        self.assertTrue(sum == len(self.data))
        self.assertTrue(len(repy.clusters) == repy.num_clusters)
        for i in range(len(repy.clusters)):
            for c in repy.clusters[i]:
                self.assertTrue(self.labels[self.data.index(c)] == i)
            
    def testReps(self):
        repy = rep.representatives(self.data, self.labels)
        repy.list_clusters()
        repy.get_reps()
        self.assertTrue(len(repy.reps) == len(repy.clusters))
        clusters = [0]
        now = time.time()
        tripa = etatc._createTripEntry(self, now, now, [1,2], [3,4])
        tripb = etatc._createTripEntry(self, now, now, [9,10], [5,8])
        tripc = etatc._createTripEntry(self, now, now, [5,6], [4,6])
        data = [tripa, tripb, tripc]
        labels = [0,0,0]
        repy = rep.representatives(data, labels)
        repy.list_clusters()
        repy.get_reps()
        logging.debug("repy.reps[0].data.start_loc = %s" % repy.reps[0].data.start_loc)
        self.assertEqual(repy.reps[0].data.start_loc.coordinates[0], 5)
        self.assertEqual(repy.reps[0].data.start_loc.coordinates[1], 6)
        self.assertEqual(repy.reps[0].data.end_loc.coordinates[0], 4)
        self.assertEqual(repy.reps[0].data.end_loc.coordinates[1], 6)

    def testLocations(self):
        repy = rep.representatives(self.data, self.labels)
        repy.list_clusters()
        repy.get_reps()
        repy.locations()
        total = 0
        for bin in repy.bins:
            for i in range(len(bin)):
                b = bin[i]
                if b[0] == 'start':
                    a = repy.reps[b[1]].data.start_loc
                if b[0] == 'end':
                    a = repy.reps[b[1]].data.end_loc
                for j in range(i):
                    c = bin[j]
                    if c[0] == 'start':
                        d = repy.reps[c[1]].data.start_loc
                    if c[0] == 'end':
                        d = repy.reps[c[1]].data.end_loc
                    self.assertTrue(repy.distance(a.coordinates[1], a.coordinates[0],
                                                  d.coordinates[1], d.coordinates[0]) < 300)
            total += len(bin)
        self.assertTrue(total == 2 * repy.num_clusters)
        for i in range(repy.num_clusters):
            self.assertTrue(sum(bin.count(('start',i)) for bin in repy.bins) == 1)
            self.assertTrue(sum(bin.count(('end',i)) for bin in repy.bins) == 1)
        self.assertTrue(len(repy.locs) == len(repy.bins))

        now = time.time()
        tripa = etatc._createTripEntry(self, now, now, [1,2], [30,40])
        tripb = etatc._createTripEntry(self, now, now, [1.0000002,2.0000002], [55.0000002,85.0000002])
        tripc = etatc._createTripEntry(self, now, now, [30.0000002,40.0000002], [55,85])
        data = [tripa, tripb, tripc]
        labels = [0,1,2]
        repy = rep.representatives(data, labels)
        repy.list_clusters()
        repy.get_reps()
        repy.locations()
        self.assertEqual(repy.bins[0], [('start', 0), ('start', 1)])
        self.assertEqual(repy.bins[1], [('end', 0), ('start', 2)])
        self.assertEqual(repy.bins[2], [('end', 1), ('end', 2)])
        self.assertAlmostEqual(repy.locs[0][0], 1.0000001, places=7)
        self.assertAlmostEqual(repy.locs[0][1], 2.0000001, places=7)
        self.assertAlmostEqual(repy.locs[1][0], 30.0000001, places=7)
        self.assertAlmostEqual(repy.locs[1][1], 40.0000001, places=7)
        self.assertAlmostEqual(repy.locs[2][0], 55.0000001, places=7)
        self.assertAlmostEqual(repy.locs[2][1], 85.0000001, places=7)

    def testClusterDict(self):
        repy = rep.representatives(self.data, self.labels)
        repy.list_clusters()
        repy.get_reps()
        repy.locations()
        repy.cluster_dict()
        self.assertTrue(len(repy.self_loops_tour_dict) == len(set(self.labels)))
        for i in range(len(repy.self_loops_tour_dict)):
            cluster = repy.self_loops_tour_dict[i]
            self.assertTrue(len(cluster['sections']) == len(repy.clusters[i]))
            self.assertTrue(('start', i) in repy.bins[cluster['start']])
            self.assertTrue(('end', i) in repy.bins[cluster['end']])
            for d in repy.clusters[i]:
                tripid = d.get_id()
                tripy = next((x for x in cluster['sections'] if x.get_id() == tripid), None)
                self.assertTrue(tripy)
                self.assertTrue(sum(sum(t.get_id() == tripid for t in cluster['sections']) for cluster in repy.self_loops_tour_dict) == 1)
        
        for c in repy.tour_dict:
            self.assertTrue(c['start'] != c['end'])


    def testMatch(self):
        now = time.time()
        tripa = etatc._createTripEntry(self, now, now, [1,2], [3,4])
        tripb = etatc._createTripEntry(self, now, now, [3,4], [1,2])
        tripc = etatc._createTripEntry(self, now, now, [1,2], [9,10])

        data = [tripa, tripb, tripc]
        labels = [0,1,2]
        repy = rep.representatives(data, labels)
        repy.list_clusters()
        repy.get_reps()

        bin = [('start', 0), ('start', 2)]
        self.assertTrue(repy.match('end', 1, bin))
        bin = [('start',0), ('end', 0)]
        self.assertTrue(not repy.match('end', 1, bin))
        bin = [('start', 0), ('start', 1)]
        self.assertTrue(not repy.match('end', 2, bin))

if __name__ == "__main__":
    etc.configLogging()
    unittest.main()
