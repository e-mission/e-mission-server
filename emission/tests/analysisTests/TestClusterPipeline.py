import unittest
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.cluster_pipeline as cp
import emission.simulation.trip_gen as tg
import uuid as uu

class ClusterPipelineTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        db = edb.get_trip_db()
        #if db.count() == 0:
        #    tg.create_fake_trips()

    def setUp(self):
        edb.get_trip_db().drop()
        edb.get_section_db().drop()
        edb.get_trip_new_db().drop()
        edb.get_section_new_db().drop()

    def tearDown(self):
        pass

    def testSanityCheckOld(self):
        # Call with a username unique to your database
        user_name = "user1"
        tg.create_fake_trips(user_name)
        thing = cp.main(user_name)

    def testSanityCheckNew(self):
        user_name = "user1"
        tg.create_fake_trips(user_name, True)
        thing = cp.main(user_name, False)    


    def testReadData(self):
        db = edb.get_fake_trips_db()
        trips = db.find()
        uuids = set()
        for t in trips:
            uuids.add(t['user_id'])
        if len(uuids) == 0:
            uuids.add(None)
        self.uuids = uuids

        data = cp.read_data(size=10)
        print 'there are ' + str(len(data))
        self.assertTrue(len(data) == 10 or len(data) == 0) #len(data)==0 if the test is run on an empty database
        uuid = 'baduuid'
        data = cp.read_data(uuid=uuid)
        self.assertTrue(not data)
        uuid = self.uuids.pop()
        data = cp.read_data(uuid = uuid)
        self.assertTrue(len(data) <= db.find({'user_id' : uuid}).count())
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
        self.assertTrue(len(newdata) == len(bins) == 0) #checking that the code doesn't crash on an empty dataset
        newdata, bins = cp.remove_noise(data, 100)
        self.assertTrue(len(newdata) <= len(data))

    def testCluster(self):
        data = cp.read_data(size=100)
        clusters, labels, newdata = cp.cluster([], 10)
        self.assertTrue(len(newdata) == clusters == len(labels) == 0) #checking that the code doesn't crash on an empty dataset
        clusters, labels, newdata = cp.cluster(data, 10)
        self.assertTrue(clusters == 0 or 10 <= clusters <= 15)
        self.assertTrue(len(labels) == len(newdata))
        self.assertTrue(cmp(newdata, data) == 0)
        data, bins = cp.remove_noise(data, 200)
        clusters, labels, newdata = cp.cluster(data, 20)
        self.assertTrue(clusters == 0 or 20 <= clusters <= 30)

    def testClusterToTourModel(self):
        data = cp.cluster_to_tour_model(None, None) #for a negative test case
        self.assertTrue(not data) #checking that the code doesn't crash on an empty dataset
        user_name = "test1"
        tg.create_fake_trips(user_name)
        data = cp.read_data(size=100) #this and the following lines form the positive test case
        data, bins = cp.remove_noise(data, 300)
        n, labels, data = cp.cluster(data, len(bins))
        tour_dict = cp.main()
        print 'n = %s | len(tour_dict) = %s' % (n, len(tour_dict))
        self.assertTrue(len(tour_dict) <= n)


    def testReadDataNew(self):
        user_name = "username"
        tg.create_fake_trips(user_name, True)
        db = edb.get_trip_new_db()
        trips = db.find()
        uuids = set()
        for t in trips:
            if 'user_id' in t:
                uuids.add(t['user_id'])
        if len(uuids) == 0:
            uuids.add(None)
        self.uuids = uuids

        data = cp.read_data(size=10, old=False)
        print 'there are ' + str(len(data))
        self.assertTrue(len(data) == 10 or len(data) == 0) #len(data)==0 if the test is run on an empty database
        uuid = 'baduuid'
        data = cp.read_data(uuid=uuid, old=False)
        self.assertTrue(not data)
        uuid = self.uuids.pop()
        data = cp.read_data(uuid=uuid, old=False)
        self.assertTrue(len(data) <= db.find({'user_id' : uuid}).count())
        self.uuids.add(uuid)
        sum = 0
        for uuid in self.uuids:
            data = cp.read_data(uuid=uuid, old=False)
            sum += len(data)
        data = cp.read_data(uuid=uuid, old=False)
        self.assertTrue(0 <= len(data) <= db.find().count())
        print "len(data) = %s" % len(data)
        self.assertTrue(len(data) == sum)


    def testRemoveNoiseNew(self):
        user_name = "djkdfj"
        tg.create_fake_trips(user_name, True)
        data = cp.read_data(size=100, old=False)
        newdata, bins = cp.remove_noise(None, 200, old=False)
        self.assertTrue(len(newdata) == len(bins) == 0) #checking that the code doesn't crash on an empty dataset
        newdata, bins = cp.remove_noise(data, 100, old=False)
        self.assertTrue(len(newdata) <= len(data))

    def testClusterNew(self):
        user_name = "dkjdjd"
        tg.create_fake_trips(user_name, True)
        data = cp.read_data(size=100, old=False)
        clusters, labels, newdata = cp.cluster([], 10, old=False)
        self.assertTrue(len(newdata) == clusters == len(labels) == 0) #checking that the code doesn't crash on an empty dataset
        clusters, labels, newdata = cp.cluster(data, 10, old=False)
        self.assertTrue(clusters == 0 or 10 <= clusters <= 15)
        self.assertTrue(len(labels) == len(newdata))
        self.assertTrue(cmp(newdata, data) == 0)
        data, bins = cp.remove_noise(data, 200, old=False)
        clusters, labels, newdata = cp.cluster(data, 20, old=False)
        self.assertTrue(clusters == 0 or 20 <= clusters <= 30)

    def testClusterToTourModelNew(self):
        tg.create_fake_trips(new=True)
        data = cp.cluster_to_tour_model(None, None, old=False) #for a negative test case
        self.assertTrue(not data) #checking that the code doesn't crash on an empty dataset
        user_name = "test1"
        tg.create_fake_trips(user_name, True)
        data = cp.read_data(uuid=user_name, size=100, old=False) #this and the following lines form the positive test case
        data, bins = cp.remove_noise(data, 300, old=False)
        n, labels, data = cp.cluster(data, len(bins), old=False)
        tour_dict = cp.main(uuid=user_name, old=False)
        print 'n = %s | len(tour_dict) = %s' % (n, len(tour_dict))
        self.assertTrue(len(tour_dict) <= n)
        

if __name__ == "__main__":
    unittest.main()


