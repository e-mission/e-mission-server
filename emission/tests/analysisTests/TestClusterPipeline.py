import unittest
import emission.core.get_database as edb
import sys
import emission.analysis.modelling.tour_model.cluster_pipeline as cp
import emission.simulation.trip_gen as tg

class ClusterPipelineTests(unittest.TestCase):
    def setUp(self):
        #set input.json
        db = edb.get_fake_trips_db()
        if db.count() == 0:
            tg.create_fake_trips()
        self.uuids = set()
        sections = db.find({'user_id' : {'$ne' : 'null'}})
        for s in sections:
            self.uuids.add(s['user_id'])

    def tearDown(self):
        return

    def testReadData(self):
        uuid = 'baduuid'
        db = edb.get_fake_trips_db()
        try:
            cp.read_data(uuid=uuid)
        except KeyError:
            self.assertTrue(True)
        except Exception:
            self.assertTrue(False)
        uuid = self.uuids[0]
        data, colors = cp.read_data(uuid=uuid)
        self.assertTrue(len(data) == db.find({'user_id' : uuid}).count()) #make sure fake trips work
        data, colors = cp.read_data()
        self.assertTrue(len(data) == db.find().count())
        d = data[0]
        self.assertTrue(d.trip_start_location and d.trip_end_location and d.start_time)


    def testRemoveNoise(self):
        data, colors = cp.read_data()
        newdata, newcolors = cp.remove_noise(data, 0, 100)
        self.assertTrue(len(newdata) == 0)
        newdata, newcolors = cp.remove_noise(data, 1, 100)
        self.assertTrue(len(newdata) == len(data))

    def testCluster(self):
        data, colors = cp.read_data()
        clusters, labels, newdata = cp.cluster(data)
        self.assertTrue(len(data)/7.0 <= clusters <= len(data)/4.0)
        self.assertTrue(len(labels) == len(newdata))
        self.assertTrue(cmp(newdata, data) == 0)
        data, colors = cp.remove_noise(data, .5, 200)
        clusters, labels, newdata = cp.cluster(data)

    def testClusterToTourModel(self):
        tour_graph = cp.main()


if __name__ == "__main__":
    unittest.main()


