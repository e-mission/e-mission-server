import unittest
import emission.core.get_database as edb
import sys
import emission.analysis.modelling.tour_model.cluster_pipeline as cp


class ClusterPipelineTests(unittest.TestCase):
    def setUp(self):
        db = edb.get_section_db()
        self.uuids = set()
        sections = db.find({'user_id' : {'$exists' : True}, 'section_start_point' : {'$exists' : True}, 'section_end_point' : {'$exists' : True}})
        for s in sections:
            self.uuids.add(s['user_id'])

    def tearDown(self):
        return

    def testReadData(self):
        uuid = 'baduuid'
        db = edb.get_section_db()
        try:
            cp.read_data(uuid)
        except KeyError:
            self.assertTrue(True)
        except Exception:
            self.assertTrue(False)
        for uuid in self.uuids:
            data, colors = cp.read_data(uuid)
            self.assertTrue(len(data) == db.find({'user_id' : uuid}).count())

    def testRemoveNoise(self):
        uuid = self.uuids.pop()
        data, colors = cp.read_data(uuid)
        newdata, newcolors = cp.remove_noise(data, 0, 100)
        self.assertTrue(len(newdata) == 0)
        newdata, newcolors = cp.remove_noise(data, 1, 100)
        self.assertTrue(len(newdata) == len(data))
        self.uuids.add(uuid)

    def testCluster(self):
        uuid = self.uuids.pop()
        data, colors = cp.read_data(uuid)
        clusters, labels, newdata = cp.cluster(data)
        self.assertTrue(len(data)/7.0 <= clusters <= len(data)/4.0)
        self.assertTrue(len(labels) == len(newdata))
        self.assertTrue(cmp(newdata, data) == 0)
        data, colors = cp.remove_noise(data, .5, 200)
        clusters, labels, newdata = cp.cluster(data)
        self.uuids.add(uuid)

if __name__ == "__main__":
    unittest.main()


