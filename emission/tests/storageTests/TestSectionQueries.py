from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import datetime as pydt
import logging
import uuid
import json
import bson.objectid as boi

# Our imports
import emission.storage.decorations.section_queries as esds
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.abstract_timeseries as esta

import emission.core.wrapper.section as ecws
import emission.core.get_database as edb
import emission.core.wrapper.entry as ecwe
import emission.tests.common as etc
import emission.pipeline.intake_stage as epi
import emission.core.wrapper.modeprediction as ecwm

# Our testing imports
import emission.tests.storageTests.analysis_ts_common as etsa

class TestSectionQueries(unittest.TestCase):
    def setUp(self):
        self.testUserId = uuid.uuid3(uuid.NAMESPACE_URL, "mailto:test@test.me")
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.testUserId})
        self.test_trip_id = "test_trip_id"

        self.testEmail = "user1"
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-21")
        self.testUUID1 = self.testUUID

        self.testEmail = "user2"
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-27")
        self.testUUID2 = self.testUUID

    def tearDown(self):
        self.clearRelatedDb()

    def clearRelatedDb(self):
        edb.get_timeseries_db().delete_many({"user_id": self.testUserId})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUserId}) 

        edb.get_timeseries_db().delete_many({"user_id": self.testUUID1})
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID2})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID1})   
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID2})   
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID1})
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID2})
        edb.get_uuid_db().delete_one({"user_email": "user1"})
        edb.get_uuid_db().delete_one({"user_email": "user2"})
        
    def testQuerySections(self):
        new_section = ecws.Section()
        new_section.start_ts = 5
        new_section.end_ts = 6
        new_section.trip_id = self.test_trip_id
        esta.TimeSeries.get_time_series(self.testUserId).insert_data(self.testUserId,
                                                                esda.RAW_SECTION_KEY,
                                                                new_section)
        ret_arr_one = esds.get_sections_for_trip(self.testUserId, self.test_trip_id)
        self.assertEqual(len(ret_arr_one), 1)
        self.assertEqual([entry.data for entry in ret_arr_one], [new_section])
        ret_arr_list = esds.get_sections_for_trip_list(self.testUserId, [self.test_trip_id])
        self.assertEqual(ret_arr_one, ret_arr_list)
        ret_arr_time = esda.get_objects(esda.RAW_SECTION_KEY, self.testUserId,
            estt.TimeQuery("data.start_ts", 4, 6))
        self.assertEqual([entry.data for entry in ret_arr_list], ret_arr_time)

    def testCleaned2InferredSectionList(self):

        # Running the pipeline for the two user datasets
        epi.run_intake_pipeline_for_user(self.testUUID1, skip_if_no_new_data = False)
        epi.run_intake_pipeline_for_user(self.testUUID2, skip_if_no_new_data = False)

        # Fetching the timeseries entries containing both raw data and analysis data after running intake pipeline
        ts_agg = esta.TimeSeries.get_aggregate_time_series()
        
        # Preparing section_user_list of sections and user_ids dictionary to be passed as function parameter
        doc_cursor = ts_agg.find_entries([esda.CLEANED_SECTION_KEY])
        sections_entries = [ecwe.Entry(doc) for doc in doc_cursor]
        section_user_list = []
        for i, section in enumerate(sections_entries):
            section_id = section.get_id()
            user_section_id = section['user_id']
            section_dict = {'section' : section_id, 'user_id' : user_section_id}
            section_user_list.append(section_dict)
        
        # Testcase 1: Aggregate timeseries entries with list of sections-user dictionary for multiple users
        # Number of predicted_entries based on the inferred sections should match the number of cleaned sections
        # Total = 25 = 10 (UUID1) + 15 (UUID2)
        curr_predicted_entries = esds.cleaned2inferred_section_list(section_user_list)
        self.assertEqual(len(curr_predicted_entries), len(sections_entries))

        # Testcase 2: Null user_id value is passed
        curr_predicted_entries = esds.cleaned2inferred_section_list([{'section' : section_id, 'user_id' : ''}])
        self.assertEqual(curr_predicted_entries, {str(section_id): ecwm.PredictedModeTypes.UNKNOWN})
        
        # Testcase 3: Null section_id value is passed
        curr_predicted_entries = esds.cleaned2inferred_section_list([{'section' : '', 'user_id' : user_section_id}])
        self.assertEqual(curr_predicted_entries, {'': ecwm.PredictedModeTypes.UNKNOWN})
        
        # Testcase 4: Empty dictionary is passed
        # Python assigns 'None' as the default key value for empty dict {}
        curr_predicted_entries = esds.cleaned2inferred_section_list([{}])
        self.assertEqual(curr_predicted_entries, {'None': ecwm.PredictedModeTypes.UNKNOWN})

        # Testcase 5: Empty list is passed
        curr_predicted_entries = esds.cleaned2inferred_section_list([])
        self.assertEqual(curr_predicted_entries, {})


if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
