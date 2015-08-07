import unittest
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.cluster_pipeline as cp
import emission.simulation.trip_gen as tg
import uuid as uu

class ClusterPipelineTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        db = edb.get_trip_db()
        if db.count() == 0:
            tg.create_fake_trips()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testReadData(self):
        db = edb.get_trip_db()
        trips = db.find()
        uuids = set()
        for t in trips:
            uuids.add(t['user_id'])
        if len(uuids) == 0:
            uuids.add(None)
        self.uuids = uuids

        data = cp.read_data(size=10)
        self.assertTrue(len(data) == 10 or len(data) == 0)
        uuid = 'baduuid'
        data = cp.read_data(uuid=uuid)
        self.assertTrue(not data)
        uuid = self.uuids.pop()
        data = cp.read_data(uuid = uuid)
        self.assertTrue(len(data) <= db.find({'user_id' : uuid}))
        self.uuids.add(uuid)
        sum = 0
        for uuid in self.uuids:
            data = cp.read_data(uuid=uuid)
            sum += len(data)
        data = cp.read_data()
        self.assertTrue(0 <= len(data) <= db.find().count())
        self.assertTrue(len(data) == sum)
        for d in data:
            self.assertTrue(d.trip_start_location and d.trip_end_location and d.start_time)

    def testRemoveNoise(self):
        data = cp.read_data(size=100)
        newdata, bins = cp.remove_noise(None, 200)
        self.assertTrue(len(newdata) == len(bins) == 0)
        newdata, bins = cp.remove_noise(data, 100)
        self.assertTrue(len(newdata) <= len(data))

    def testCluster(self):
        data = cp.read_data(size=100)
        clusters, labels, newdata = cp.cluster([], 10)
        self.assertTrue(len(newdata) == clusters == len(labels) == 0)
        clusters, labels, newdata = cp.cluster(data, 10)
        self.assertTrue(clusters == 0 or 10 <= clusters <= 15)
        self.assertTrue(len(labels) == len(newdata))
        self.assertTrue(cmp(newdata, data) == 0)
        data, bins = cp.remove_noise(data, 200)
        clusters, labels, newdata = cp.cluster(data, 20)
        self.assertTrue(clusters == 0 or 20 <= clusters <= 30)

    def testClusterToTourModel(self):
        data = cp.cluster_to_tour_model(None, None)
        self.assertTrue(not data)
        data = cp.read_data(size=100)
        data, bins = cp.remove_noise(data, 300)
        n, labels, data = cp.cluster(data, len(bins))
        tour_dict = cp.main()
        self.assertTrue(len(tour_dict) <= n)
        

if __name__ == "__main__":
    unittest.main()


