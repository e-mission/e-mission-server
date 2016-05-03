import unittest
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.similarity as similarity
import emission.simulation.trip_gen as tg
import math
from emission.core.wrapper.trip_old import Trip, Coordinate
import emission.analysis.modelling.tour_model.cluster_pipeline as cp
import datetime 
import os, os.path

class SimilarityTests(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(SimilarityTests, self).__init__(*args, **kwargs)
        self.data = cp.read_data(size=100)
        #if len(self.data) == 0:
        #    tg.create_fake_trips()
        #    self.data = cp.read_data(size=100)
        print 'there are ' + str(len(self.data))

    def setUp(self):
        pass

    def tearDown(self):
        return


    def testInit(self):
        try:
            sim = similarity.similarity([], 'b')
        except ValueError:
            self.assertTrue(True)
        except Exception:
            self.assertTrue(False)

        sim = similarity.similarity([], 100)
        self.assertTrue(len(sim.data) == 0)
        now = datetime.datetime.now()
        start = Coordinate(47,-122)
        end = Coordinate(47,-123)
        t1 = Trip(None, None, None, None, now, now, start, start)
        t2 = Trip(None, None, None, None, now, now, start, end)
        sim = similarity.similarity([t1, t2], 100)
        simmy = similarity.similarity([t2], 100)
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
        now = datetime.datetime.now()
        start = Coordinate(47,-122)
        end = Coordinate(47,-123)
        for i in range(10):
            a = Trip(None, None, None, None, now, now, start, end)
            data.append(a)
        start = Coordinate(41,-74)
        end = Coordinate(42, -74)
        for i in range(10):
            a = Trip(None, None, None, None, now, now, start, end)
            data.append(a)
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
        start = Coordinate(47,-122)
        end = Coordinate(47,-123)
        now = datetime.datetime.now()
        t = Trip(None, None, None, None, now, now, start, end)
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
        start = Coordinate(-122.259447, 37.875174)
        end1 = Coordinate(-122.259279, 37.875479)
        end2 = Coordinate(-122.252287, 37.869569)
        now = datetime.datetime.now()
        t1 = Trip(None, None, None, None, now, now, start, end1)
        t2 = Trip(None, None, None, None, now, now, start, end2)
        sim = similarity.similarity(self.data, 300)
        self.assertTrue(sim.distance(start.lat, start.lon, end1.lat, end1.lon))
        self.assertTrue(not sim.distance(start.lat, start.lon, end2.lat, end2.lon))

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
        self.assertTrue(os.path.isfile('./histogram.png'))
        os.remove('./histogram.png')

    def testEvaluateBins(self):
        sim = similarity.similarity([], 300)
        a = sim.evaluate_bins()
        self.assertTrue(not a)
        sim = similarity.similarity(self.data, 300)
        b = sim.evaluate_bins()
        self.assertTrue(not b)
        sim.bin_data()
        c = sim.evaluate_bins()
        if sim.data:
            self.assertTrue(c)

if __name__ == "__main__":
    unittest.main()
