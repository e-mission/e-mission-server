from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import logging
import json
import datetime as pydt

# Our imports
import emission.core.get_database as edb
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss

import emission.storage.decorations.timeline as esdt
import emission.storage.decorations.trip_queries as esdtq

import emission.core.wrapper.rawplace as ecwrp
import emission.core.wrapper.rawtrip as ecwrt
import emission.core.wrapper.stop as ecws
import emission.core.wrapper.section as ecwsc
import emission.core.wrapper.localdate as ecwl
import emission.core.wrapper.untrackedtime as ecwut

import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.storage.timeseries.format_hacks.move_filter_field as estfm

# Test imports
import emission.tests.common as etc

class TestTimeline(unittest.TestCase):
    def setUp(self):
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-27")
        eaicf.filter_accuracy(self.testUUID)
        estfm.move_all_filters_to_data()        
        logging.info("After loading, timeseries db size = %s" % edb.get_timeseries_db().estimated_document_count())
        self.day_start_ts = 1440658800
        self.day_end_ts = 1440745200
        self.day_start_dt = ecwl.LocalDate({'year': 2015, 'month': 8, 'day': 27})
        self.day_end_dt = ecwl.LocalDate({'year': 2015, 'month': 8, 'day': 27})

    def tearDown(self):
        self.clearRelatedDb()

    def clearRelatedDb(self):
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})

    @staticmethod
    def get_type(element):
        logging.debug("getting type for %s" % element)
        return type(element)

    def checkPlaceTripConsistency(self, tl):
        prev_type = None
        prev_element = None
        checked_count = 0
        i = 0
        for i, curr_element in enumerate(tl):
            # logging.debug("%s: %s" % (i, curr_element))
            curr_type = self.get_type(curr_element.data)
            if prev_type is not None:
                checked_count += 1
                self.assertNotEqual(prev_type, curr_type)
                if prev_type == ecwrp.Rawplace:
                    self.assertEqual(prev_element.data.starting_trip,
                                     curr_element.get_id())
                else:
                    self.assertTrue(prev_type == ecwrt.Rawtrip or
                                    prev_type == ecwut.Untrackedtime)
                    self.assertEqual(prev_element.data.end_place,
                                     curr_element.get_id())
            prev_type = curr_type
            prev_element = curr_element
        self.assertEqual(checked_count, i)

    def testDatetimeTimeline(self):
        eaist.segment_current_trips(self.testUUID)
        tl = esdt.get_raw_timeline_from_dt(self.testUUID,
                                           self.day_start_dt, self.day_end_dt)
        self.checkPlaceTripConsistency(tl)

    def testPlaceTripTimeline(self):
        eaist.segment_current_trips(self.testUUID)
        tl = esdt.get_raw_timeline(self.testUUID, self.day_start_ts, self.day_end_ts)
        self.checkPlaceTripConsistency(tl)

    def testStopSectionTimeline(self):
        eaist.segment_current_trips(self.testUUID)
        eaiss.segment_current_sections(self.testUUID)
        tl = esdt.get_raw_timeline(self.testUUID, self.day_start_ts, self.day_end_ts)


        for i, element in enumerate(tl):
            logging.debug("%s: %s" % (i, type(element)))
            curr_type = self.get_type(element)
            if curr_type == ecwrt.Rawtrip:
                curr_tl = esdtq.get_raw_timeline_for_trip(self.testUUID, element.get_id())
                logging.debug("Got timeline %s for trip %s" % (curr_tl, element.start_fmt_time))
                prev_sub_type = None
                prev_element = None
                checked_count = 0
                j = 0
                for j, curr_element in enumerate(curr_tl):
                    logging.debug("curr_element = %s" % curr_element)
                    curr_sub_type = self.get_type(curr_element)
                    if prev_sub_type is not None:
                        checked_count = checked_count + 1
                        self.assertNotEqual(prev_sub_type, curr_sub_type)
                        if prev_sub_type == ecws.Stop:
                            self.assertEqual(prev_element.starting_section, curr_element.get_id())
                        else:
                            self.assertEqual(prev_sub_type, ecwsc.Section)
                            self.assertEqual(prev_element.end_stop, curr_element.get_id())
                    prev_sub_type = curr_sub_type
                    prev_element = curr_element
                self.assertEqual(checked_count, j)


if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
