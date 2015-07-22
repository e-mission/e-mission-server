import unittest
import emission.core.get_database as edb
import sys
import emission.analysis.modelling.tour_model.similarity as similarity
import emission.simulation.trip_gen as tg
import math

class SimilarityTests(unittest.TestCase):

    def setUp(self):
        #tg.create_fake_trips()
        
        db = edb.get_fake_trips_db()
        
        trips = db.find()
        self.data = list(trips)

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
        sim = similarity.similarity([], .5, 300)
        sim.bin_data()
        sim.graph()
        sim = similarity.similarity(self.data, .5, 300)
        sim.graph()
        sim.bin_data()
        sim.graph()
        sim.delete_bins()
        sim.graph()

    def testMapBins(self):
        sim = similarity.similarity([], .5, 300)
        sim.bin_data()
        sim.map_bins()
        sim = similarity.similarity(self.data, .5, 300)
        sim.map_bins()
        sim.bin_data()
        sim.map_bins()
        sim.delete_bins()
        sim.map_bins()

    def testEvaluateBins(self):
        sim = similarity.similarity([], .5, 300)
        sim.evaluate_bins()
        sim = similarity.similarity(self.data, .5, 300)
        sim.evaluate_bins()
        sim.bin_data()
        sim.evaluate_bins()

if __name__ == "__main__":
    unittest.main()
