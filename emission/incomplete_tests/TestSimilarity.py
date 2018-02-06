from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import range
from builtins import *
import logging
import unittest
import uuid
import time
import datetime 
import os, os.path

import emission.tests.analysisTests.tourModelTests.common as etatc
import emission.tests.common as etc

import emission.core.get_database as edb

import emission.analysis.modelling.tour_model.similarity as similarity
import emission.analysis.modelling.tour_model.cluster_pipeline as cp

import emission.storage.timeseries.abstract_timeseries as esta

class SimilarityTests(unittest.TestCase):

    def __init__(self,  *args, **kwargs):
        super(SimilarityTests, self).__init__(*args, **kwargs)

    def setUp(self):
        self.testUUID = uuid.uuid4()
        self.data = cp.read_data()
        #if len(self.data) == 0:
        #    tg.create_fake_trips()
        #    self.data = cp.read_data(size=100)
        logging.info("Found %s trips" % len(self.data))
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)

    def tearDown(self):
        edb.get_timeseries_db().remove({'user_id': self.testUUID})
        edb.get_analysis_timeseries_db().remove({'user_id': self.testUUID})

    def testInit(self):
        try:
            sim = similarity.similarity([], 'b')
        except ValueError:
            self.assertTrue(True)
        except Exception:
            self.assertTrue(False)

        logging.debug("STARTING init test")
        sim = similarity.similarity([], 100)
        self.assertTrue(len(sim.data) == 0)
        now = time.time()
        start = [-122,47]
        end = [-123,47]
        t1 = etatc._createTripEntry(self, now, now, start, start)
        t2 = etatc._createTripEntry(self, now, now, start, end)
        sim = similarity.similarity([t1, t2], 100)
        logging.debug("sim.data = %s" % sim.data)
        simmy = similarity.similarity([t2], 100)
        logging.debug("simmy.data = %s" % simmy.data)
        self.assertTrue(sim.data == simmy.data)

    def testBinData(self):
        sim = similarity.similarity([], 300)
        self.assertTrue(not sim.bins)
        sim = similarity.similarity(self.data, 300)
        sim.bin_data()
        sum = 0
        for bin in sim.bins:
            sum += len(bin)
        self.assertTrue(sum == len(sim.data))
        testbins = set()
        for bin in sim.bins:
            for b in bin:
                testbins.add(b)
        self.assertTrue(len(testbins) == len(sim.data))
        for i in range(len(sim.bins)-1):
            self.assertTrue(len(sim.bins[i]) >= len(sim.bins[i+1]))

        data = []
        now = time.time()
        start = [-122, 47]
        end = [-123, 47]
        for i in range(10):
            data.append(etatc._createTripEntry(self, now, now, start, end))
        start = [-74, 41]
        end = [-74, 42]
        for i in range(10):
            data.append(etatc._createTripEntry(self, now, now, start, end))
        sim = similarity.similarity(data, 300)
        sim.bin_data()
        self.assertTrue(len(sim.bins) == 2)

    def testDeleteBins(self):
        sim = similarity.similarity(self.data, 300)
        sim.bin_data()
        sim.delete_bins()
        b = len(sim.bins)
        if sim.data:
            self.assertTrue(b == sim.num)

    def testElbowDistance(self):
        start = [-122,47]
        end = [-123,47]
        now = time.time()
        t = etatc._createTripEntry(self, now, now, start, end)
        data = [t] * 11
        bins = [[1,2,3,4], [5,6,10], [7], [8], [9], [0]]
        sim = similarity.similarity(data, 300)
        sim.bins = bins
        sim.delete_bins()
        self.assertTrue(sim.num == 2)

    def testMatch(self):
        sim = similarity.similarity(self.data, 300)
        sim.bin_data()
        for bin in sim.bins:
            for b in bin:
                for c in bin:
                    self.assertTrue(sim.distance_helper(b,c))

    def testDistance(self):
        start = [-122.259447, 37.875174]
        end1 = [-122.259279, 37.875479]
        end2 = [-122.252287, 37.869569]
        now = time.time()
        t1 = etatc._createTripEntry(self, now, now, start, end1)
        t2 = etatc._createTripEntry(self, now, now, start, end2)
        sim = similarity.similarity(self.data, 300)
        self.assertTrue(sim.distance(start[1], start[0], end1[1], end1[0]))
        self.assertTrue(not sim.distance(start[1], start[0], end2[1], end2[0]))

    def testGraph(self):
        if os.path.isfile('./histogram.png'):
            os.remove('./histogram.png')
        sim = similarity.similarity([], 300)
        sim.bin_data()
        # sim.graph()
        sim = similarity.similarity(self.data, 300)
        # sim.graph()
        sim.bin_data()
        # sim.graph()
        sim.delete_bins()
        # sim.graph()
        # self.assertTrue(os.path.isfile('./histogram.png'))
        # os.remove('./histogram.png')

    def testEvaluateBins(self):
        sim = similarity.similarity([], 300)
        a = sim.evaluate_bins()
        self.assertTrue(not a)
        sim = similarity.similarity(self.data, 300)
        sim.bin_data()
        c = sim.evaluate_bins()
        if sim.data:
            self.assertTrue(c)

if __name__ == "__main__":
    etc.configLogging()
    unittest.main()
