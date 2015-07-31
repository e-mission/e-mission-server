import unittest
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.similarity as similarity
import emission.simulation.trip_gen as tg
import math
from emission.core.wrapper.trip import Trip, Coordinate
import emission.analysis.modelling.tour_model.cluster_pipeline as cp
import datetime 
import os, os.path

class SimilarityTests(unittest.TestCase):

    def setUp(self):
        self.data = cp.read_data()

    def tearDown(self):
        return


    def testInit(self):
        try:
            sim = similarity.similarity([], 'a', 300)
        except ValueError:
            self.assertTrue(True)
        except Exception:
            self.assertTrue(False)
        try:
            sim = similarity.similarity([], .1, 'b')
        except ValueError:
            self.assertTrue(True)
        except Exception:
            self.assertTrue(False)

        sim = similarity.similarity([], -1, 300)
        a = sim.percent
        sim = similarity.similarity([],0, 300)
        self.assertTrue(a == sim.percent)
        sim = similarity.similarity([],17000, 300)
        b = sim.percent
        sim = similarity.similarity([], 1, 300)
        self.assertTrue(b == sim.percent)


    def testBinData(self):
        sim = similarity.similarity([], .5, 300)
        self.assertTrue(not bool(sim.bins))
        sim = similarity.similarity(self.data, .5, 300)
        sim.bin_data()
        self.assertTrue(bool(sim.bins))
        testbins = set()
        for bin in sim.bins:
            for b in bin:
                testbins.add(b)
        self.assertTrue(len(testbins) == len(sim.data))
        for i in range(len(sim.bins)-1):
            self.assertTrue(len(sim.bins[i]) >= len(sim.bins[i+1]))

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
        sim = similarity.similarity(data, .5, 300)
        sim.bin_data()
        self.assertTrue(len(sim.bins) == 2)

    def testDeleteBins(self):
        sim = similarity.similarity(self.data, .5, 300)
        sim.bin_data()
        a = len(sim.bins)
        sim.delete_bins()
        b = len(sim.bins)
        self.assertTrue(b <= math.ceil(sim.percent * a))
        sim = similarity.similarity(self.data, 0, 300)
        sim.bin_data()
        sim.delete_bins()
        self.assertTrue(len(sim.bins) == 0)
        sim = similarity.similarity(self.data, 1, 300)
        sim.bin_data()
        l = len(sim.bins)
        sim.delete_bins()
        self.assertTrue(len(sim.bins) == l)

    def testMatch(self):
        sim = similarity.similarity(self.data, .5, 300)
        sim.bin_data()
        for bin in sim.bins:
            for b in bin:
                for c in bin:
                    self.assertTrue(sim.distance_helper(b,c))

    def testDistance(self):
        start = [-122.259447, 37.875174]
        end1 = [-122.259279, 37.875479]
        end2 = [-122.252287, 37.869569]
        index = len(self.data)
        self.data.append({'trip_start_location' : start, 'trip_end_location' : end1})
        self.data.append({'trip_start_location' : start, 'trip_end_location' : end2})
        sim = similarity.similarity(self.data, .5, 300)
        self.assertTrue(sim.distance(start[1], start[0], end1[1], end1[0]))
        self.assertTrue(not sim.distance(start[1], start[0], end2[1], end2[0]))

    def testGraph(self):
        if os.path.isfile('./histogram.png'):
            os.remove('./histogram.png')
        sim = similarity.similarity([], .5, 300)
        sim.bin_data()
        sim.graph()
        sim = similarity.similarity(self.data, .5, 300)
        sim.graph()
        sim.bin_data()
        sim.graph()
        sim.delete_bins()
        sim.graph()
        self.assertTrue(os.path.isfile('./histogram.png'))
        os.remove('./histogram.png')

    def testMapBins(self):
        if os.path.isfile('./mybins.html'):
            os.remove('./mybins.html')
        sim = similarity.similarity([], .5, 300)
        sim.bin_data()
        sim.map_bins()
        sim = similarity.similarity(self.data, .5, 300)
        sim.map_bins()
        sim.bin_data()
        sim.map_bins()
        sim.delete_bins()
        sim.map_bins()
        self.assertTrue(os.path.isfile('./mybins.html'))
        os.remove('./mybins.html')

    def testEvaluateBins(self):
        sim = similarity.similarity([], .5, 300)
        a = sim.evaluate_bins()
        self.assertTrue(not a)
        sim = similarity.similarity(self.data, .5, 300)
        b = sim.evaluate_bins()
        self.assertTrue(not b)
        sim.bin_data()
        c = sim.evaluate_bins()
        sim.delete_bins()
        d = sim.evaluate_bins()
        self.assertTrue(c == d)

if __name__ == "__main__":
    unittest.main()
