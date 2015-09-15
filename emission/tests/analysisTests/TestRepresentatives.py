import unittest
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.representatives as rep
import emission.simulation.trip_gen as tg
import emission.analysis.modelling.tour_model.featurization as feat
import emission.analysis.modelling.tour_model.cluster_pipeline as cp
from emission.core.wrapper.trip_old import Trip, Coordinate

class RepresentativesTests(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(RepresentativesTests, self).__init__(*args, **kwargs)
        self.data = cp.read_data(size=100)
        #if len(self.data) == 0:
        #    tg.create_fake_trips()
        #    self.data = cp.read_data(size=100)
        print 'there are ' + str(len(self.data))
        n = len(self.data)/5
        self.labels = feat.featurization(self.data).cluster(min_clusters=n, max_clusters=n)        

    def tearDown(self):
        pass

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
        tripa = Trip(None, None, None, None, None, None, Coordinate(1,2), Coordinate(3,4))
        tripb = Trip(None, None, None, None, None, None, Coordinate(9,10), Coordinate(5,8))
        tripc = Trip(None, None, None, None, None, None, Coordinate(5,6), Coordinate(4,6))
        data = [tripa, tripb, tripc]
        labels = [0,0,0]
        repy = rep.representatives(data, labels)
        repy.list_clusters()
        repy.get_reps()
        self.assertTrue(repy.reps[0].trip_start_location.lat == 5)
        self.assertTrue(repy.reps[0].trip_start_location.lon == 6)
        self.assertTrue(repy.reps[0].trip_end_location.lat == 4)
        self.assertTrue(repy.reps[0].trip_end_location.lon == 6)

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
                    a = repy.reps[b[1]].trip_start_location
                if b[0] == 'end':
                    a = repy.reps[b[1]].trip_end_location
                for j in range(i):
                    c = bin[j]
                    if c[0] == 'start':
                        d = repy.reps[c[1]].trip_start_location
                    if c[0] == 'end':
                        d = repy.reps[c[1]].trip_end_location
                    self.assertTrue(repy.distance(a.lat, a.lon, d.lat, d.lon) < 300)
            total += len(bin)
        self.assertTrue(total == 2 * repy.num_clusters)
        for i in range(repy.num_clusters):
            self.assertTrue(sum(bin.count(('start',i)) for bin in repy.bins) == 1)
            self.assertTrue(sum(bin.count(('end',i)) for bin in repy.bins) == 1)
        self.assertTrue(len(repy.locs) == len(repy.bins))

        tripa = Trip(None, None, None, None, None, None, Coordinate(1,2), Coordinate(30,40))
        tripb = Trip(None, None, None, None, None, None, Coordinate(1.0000002,2.0000002), Coordinate(55.0000002,85.0000002))
        tripc = Trip(None, None, None, None, None, None, Coordinate(30.0000002,40.0000002), Coordinate(55,85))
        data = [tripa, tripb, tripc]
        labels = [0,1,2]
        repy = rep.representatives(data, labels)
        repy.list_clusters()
        repy.get_reps()
        repy.locations()
        self.assertTrue(repy.bins[0] == [('start', 0), ('start', 1)])
        self.assertTrue(repy.bins[1] == [('end', 0), ('start', 2)])
        self.assertTrue(repy.bins[2] == [('end', 1), ('end', 2)])
        self.assertTrue(round(repy.locs[0].lat,7) == 1.0000001)
        self.assertTrue(round(repy.locs[0].lon,7) == 2.0000001)
        self.assertTrue(round(repy.locs[1].lat,7) == 30.0000001)
        self.assertTrue(round(repy.locs[1].lon,7) == 40.0000001)
        self.assertTrue(round(repy.locs[2].lat,7) == 55.0000001)
        self.assertTrue(round(repy.locs[2].lon,7) == 85.0000001)

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
                tripid = d.trip_id
                tripy = next((x for x in cluster['sections'] if x.trip_id == tripid), None)
                self.assertTrue(tripy)
                self.assertTrue(sum(sum(t.trip_id == tripid for t in cluster['sections']) for cluster in repy.self_loops_tour_dict) == 1)
        
        for c in repy.tour_dict:
            self.assertTrue(c['start'] != c['end'])


    def testMatch(self):
        tripa = Trip(None, None, None, None, None, None, Coordinate(1,2), Coordinate(3,4))
        tripb = Trip(None, None, None, None, None, None, Coordinate(3,4), Coordinate(1,2))
        tripc = Trip(None, None, None, None, None, None, Coordinate(1,2), Coordinate(9,10))

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
    unittest.main()
