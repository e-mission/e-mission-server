import unittest
import emission.core.get_database as edb
import sys
import emission.analysis.modelling.tour_model.cluster_pipeline as cp
import emission.simulation.trip_gen as tg
from emission.core.wrapper.trip import Trip, Coordinate, E_Mission_Trip
import datetime
import uuid as uu

class ClusterPipelineTests(unittest.TestCase):
    def setUp(self):
        db = edb.get_trip_db()
        if db.count() == 0:
            tg.create_fake_trips()##
        trips = db.find()
        uuids = set()
        for t in trips:
            uuids.add(t['user_id'])
        self.uuids = uuids

    def tearDown(self):
        return

    def testReadData(self):
        uuid = 'baduuid'
        db = edb.get_trip_db()
        try:
            cp.read_data(uuid=uuid)
        except KeyError:
            self.assertTrue(True)
        except Exception:
            self.assertTrue(False)
        uuid = self.uuids.pop()
        try:
            data = cp.read_data(uuid = uuid)
            self.assertTrue(len(data) <= db.find({'user_id' : uuid}))
        except KeyError:
            pass
        self.uuids.add(uuid)
        sum = 0
        for uuid in self.uuids:
            try:
                data = cp.read_data(uuid=uuid)
            except KeyError:
                continue
            sum += len(data)
        data = cp.read_data()
        self.assertTrue(0 < len(data) <= db.find().count())
        self.assertTrue(len(data) == sum)
        d = data[0]
        self.assertTrue(d.trip_start_location and d.trip_end_location and d.start_time)

    def testRemoveNoise(self):
        data = cp.read_data()
        newdata, bins = cp.remove_noise(data, 0, 100)
        self.assertTrue(len(newdata) == 0)
        newdata, bins = cp.remove_noise(data, 1, 100)
        self.assertTrue(len(newdata) == len(data))

    def testCluster(self):
        data = cp.read_data()
        clusters, labels, newdata = cp.cluster(data, 10)
        self.assertTrue(10 <= clusters <= 14)
        self.assertTrue(len(labels) == len(newdata))
        self.assertTrue(cmp(newdata, data) == 0)
        data, bins = cp.remove_noise(data, .5, 200)
        clusters, labels, newdata = cp.cluster(data, 20)
        self.assertTrue(20 <= clusters <= 1.4 * 20)

    def testClusterToTourModel(self):
        try:
            cp.cluster_to_tour_model(None, None)
        except ValueError: 
            self.assertTrue(True)
        data = cp.read_data()
        data, bins = cp.remove_noise(data, .5, 300)
        n, labels, data = cp.cluster(data, len(bins))
        tour_dict = cp.main()
        self.assertTrue(len(tour_dict) <= n)
        

if __name__ == "__main__":
    unittest.main()


