# Standard imports
import unittest
import json
import logging
from datetime import datetime, timedelta
import datetime as pydt

# Our imports
import emission.core.get_database as edb
from emission.net.api import visualize
import emission.tests.common as etc
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.storage.timeseries.format_hacks.move_filter_field as estfm

logging.basicConfig(level=logging.DEBUG)



class TestVisualize(unittest.TestCase):

  def setUp(self):
    self.clearRelatedDb()
    etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-27")
    eaicf.filter_accuracy(self.testUUID)
    estfm.move_all_filters_to_data()        
    logging.info("After loading, timeseries db size = %s" % edb.get_timeseries_db().count())
    self.day_start_ts = 1440658800
    self.day_end_ts = 1440745200
    self.day_start_dt = pydt.datetime(2015,8,27)
    self.day_end_dt = pydt.datetime(2015,8,28)

  def clearRelatedDb(self):
    edb.get_timeseries_db().remove()
    edb.get_place_db().remove()
    edb.get_stop_db().remove()

    edb.get_trip_new_db().remove()
    edb.get_section_new_db().remove()


  def testCommutePopRoute(self):
    points = visualize.Commute_pop_route("MotionTypes.BICYCLING", self.day_start_dt, self.day_end_dt)
    # I have to add test data with modes, I will do that tomorrow.

  def testBerkeleyPopRoute(self):
    points = visualize.Berkeley_pop_route(self.day_start_dt, self.day_end_dt)
    self.assertTrue(len(['latlng']) > 0)

if __name__ == '__main__':
    unittest.main()
