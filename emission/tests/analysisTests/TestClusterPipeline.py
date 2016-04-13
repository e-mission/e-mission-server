import unittest
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.cluster_pipeline as cp
import emission.simulation.trip_gen as tg
import uuid as uu
import emission.tests.common as etc
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.storage.timeseries.format_hacks.move_filter_field as estfm


class ClusterPipelineTests(unittest.TestCase):


    def setUp(self):
        self.clearRelatedDb()
        edb.get_trip_db().remove()
        edb.get_section_db().remove()
        edb.get_trip_new_db().remove()
        edb.get_section_new_db().remove()
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-21")
        eaicf.filter_accuracy(self.testUUID)
        estfm.move_all_filters_to_data()
        eaist.segment_current_trips(self.testUUID)
        eaiss.segment_current_sections(self.testUUID)



    def tearDown(self):
        pass

    def testSanityCheckNew(self):
        thing = cp.main(self.testUUID, False)    


    def testReadDataNew(self):
        db = edb.get_trip_new_db()
        trips = db.find()
        uuids = set()
        for t in trips:
            if self.testUUID in t:
                uuids.add(t[self.testUUID])
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
        data = cp.read_data(uuid=self.testUUID, size=100, old=False) #this and the following lines form the positive test case
        newdata, bins = cp.remove_noise(None, 200, old=False)
        self.assertTrue(len(newdata) == len(bins) == 0) #checking that the code doesn't crash on an empty dataset
        newdata, bins = cp.remove_noise(data, 100, old=False)
        self.assertTrue(len(newdata) <= len(data))

    def testClusterNew(self):
        data = cp.read_data(uuid=self.testUUID, size=100, old=False) #this and the following lines form the positive test case
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
        
        data = cp.cluster_to_tour_model(None, None, old=False) #for a negative test case
        self.assertTrue(not data) #checking that the code doesn't crash on an empty dataset
        user_name = "test1"
        data = cp.read_data(uuid=self.testUUID, size=100, old=False) #this and the following lines form the positive test case
        data, bins = cp.remove_noise(data, 300, old=False)
        n, labels, data = cp.cluster(data, len(bins), old=False)
        tour_dict = cp.main(uuid=user_name, old=False)
        print 'n = %s | len(tour_dict) = %s' % (n, len(tour_dict))
        self.assertTrue(len(tour_dict) <= n)
        


    def clearRelatedDb(self):
        edb.get_timeseries_db().remove()
        edb.get_place_db().remove()
        edb.get_stop_db().remove()

        edb.get_trip_new_db().remove()
        edb.get_section_new_db().remove()

if __name__ == "__main__":
    pass
    #unittest.main() outdated tests, dont want to figure out now


