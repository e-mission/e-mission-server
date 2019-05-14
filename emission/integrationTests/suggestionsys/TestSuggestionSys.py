import unittest
import datetime as pydt
import logging
import uuid
import json
import bson.json_util as bju
import numpy as np

# Our imports
import emission.core.wrapper.suggestion_sys as sugg
import emission.storage.decorations.analysis_timeseries_queries as esda

import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.abstract_timeseries as esta

import emission.net.api.usercache as enau

import emission.core.get_database as edb
import emission.core.wrapper.userlabel as ecul
import emission.core.wrapper.cleanedtrip as ecwct
import emission.core.wrapper.section as ecwc
import emission.core.wrapper.stop as ecws
import emission.core.wrapper.entry as ecwe

import emission.tests.storageTests.analysis_ts_common as etsa
import emission.tests.common as etc

class TestSuggestionSys(unittest.TestCase):
    def setUp(self):
        self.testUserId = uuid.uuid3(uuid.NAMESPACE_URL, "mailto:test@test.me")
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.testUserId})
    
    def tearDown(self):
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.testUserId})
        edb.get_usercache_db().delete_many({'user_id': self.testUserId})

    # Trip from SF -> PA
    # with a bid set to the subway at the SF caltrain station, this generates a
    # suggestion
    def create_fake_trip_to_pa(self):
        new_trip = ecwct.Cleanedtrip()
        new_trip.start_ts =10 
        new_trip.end_ts = 100
        new_trip.start_loc = {'coordinates': [-122.4002, 37.77302], 'type': 'Point'}
        new_trip.end_loc = {'coordinates': [-122.14091, 37.42872], 'type': 'Point'}
        new_trip.distance = 51663.658809878514
        new_trip_id = esta.TimeSeries.get_time_series(self.testUserId).insert_data(
            self.testUserId, esda.CLEANED_TRIP_KEY, new_trip)
        new_trip_entry = esta.TimeSeries.get_time_series(self.testUserId).get_entry_from_id(
            esda.CLEANED_TRIP_KEY, new_trip_id)
        return new_trip_entry

    # Trip from PA -> SF
    # with a bid set to the subway at the SF caltrain station, this does not
    # generate a suggestion. This is counter-intutive - the first was a trip to
    # palo alto; the second is a trip to san francisco, so shouldn't we find an
    # SF replacement for the second trip?
    # apparently, not really
    def create_fake_trip_to_sf(self):
        new_trip = ecwct.Cleanedtrip()
        new_trip.start_ts = 1000
        new_trip.end_ts = 1100
        new_trip.start_loc = {'coordinates': [-122.14091, 37.42872], 'type': 'Point'}
        new_trip.end_loc = {'coordinates': [-122.4002, 37.77302], 'type': 'Point'}
        new_trip.distance = 53859.19138604545
        new_trip_id = esta.TimeSeries.get_time_series(self.testUserId).insert_data(
            self.testUserId, esda.CLEANED_TRIP_KEY, new_trip)
        new_trip_entry = esta.TimeSeries.get_time_series(self.testUserId).get_entry_from_id(
            esda.CLEANED_TRIP_KEY, new_trip_id)
        return new_trip_entry

    # no confirmation from the user -> no suggestion
    def testSingleUnconfirmedTrip(self): 
        new_trip = self.create_fake_trip_to_pa()
        ret_sugg = sugg.calculate_yelp_server_suggestion_singletrip_nominatim(
            self.testUserId, str(new_trip.get_id()))

        self.assertIsNotNone(ret_sugg)
        self.assertIsNone(ret_sugg["businessid"])
        self.assertEqual(ret_sugg["method"], "bike")

    # confirmation from the user, but trip to SF -> no suggestion
    def testSingleConfirmedTripNoMatches(self): 
        new_trip = self.create_fake_trip_to_sf()
        confirmed_dest = {
            'metadata': {'key': 'manual/destination_confirm',
                         "write_ts": new_trip.metadata.write_ts,
                         "type": "message"
                        },
            "data": {
                "start_ts": new_trip.data.start_ts,
                "end_ts": new_trip.data.end_ts,
                "label": "subway-san-francisco-18"
            }
        }
        enau.sync_phone_to_server(self.testUserId, [confirmed_dest])
        ret_sugg = sugg.calculate_yelp_server_suggestion_singletrip_nominatim(
            self.testUserId, str(new_trip.get_id()))

        self.assertIsNotNone(ret_sugg)
        self.assertIsNone(ret_sugg["businessid"])
        self.assertEqual(ret_sugg["method"], "bike")

    # confirmation from the user, trip to PA -> suggestion
    def testSingleConfirmedTripMatch(self): 
        new_trip = self.create_fake_trip_to_pa()
        confirmed_dest = {
            'metadata': {'key': 'manual/destination_confirm',
                         "write_ts": new_trip.metadata.write_ts,
                         "type": "message"
                        },
            "data": {
                "start_ts": new_trip.data.start_ts,
                "end_ts": new_trip.data.end_ts,
                "label": "subway-san-francisco-18"
            }
        }
        enau.sync_phone_to_server(self.testUserId, [confirmed_dest])
        ret_sugg = sugg.calculate_yelp_server_suggestion_singletrip_nominatim(
            self.testUserId, str(new_trip.get_id()))

        self.assertIsNotNone(ret_sugg)
        self.assertEqual(ret_sugg["businessid"], "lous-cafe-san-francisco")
        self.assertEqual(ret_sugg["method"], 'bike')

    # confirmation from the user, trip to PA followed by trip to SF
    # trip to SF does not generate a suggestion, so we fall back to the trip to PA
    def testDefaultConfirmedTripMatch(self):
        new_trip_list = [self.create_fake_trip_to_pa(),
                         self.create_fake_trip_to_sf()]
        for new_trip in new_trip_list:
            confirmed_dest = {
                'metadata': {'key': 'manual/destination_confirm',
                             "write_ts": new_trip.metadata.write_ts,
                             "type": "message"
                            },
                "data": {
                    "start_ts": new_trip.data.start_ts,
                    "end_ts": new_trip.data.end_ts,
                    "label": "subway-san-francisco-18"
                }
            }
            enau.sync_phone_to_server(self.testUserId, [confirmed_dest])

        # The older trip (to PA) will generate a match 
        ret_sugg = sugg.calculate_yelp_server_suggestion_nominatim(self.testUserId)

        self.assertIsNotNone(ret_sugg)
        self.assertEqual(ret_sugg["businessid"], "lous-cafe-san-francisco")
        self.assertEqual(ret_sugg["method"], 'bike')
        self.assertEqual(ret_sugg["tripid"], new_trip_list[0].get_id())

    # confirmation from the user, trip to SF followed by trip to SF (this is
    # not consistent according to real world rules, but will exercise the code
    # just fine)
    # no suggestions generated
    def testDefaultConfirmedTripNoMatches(self):
        new_trip_list = [self.create_fake_trip_to_sf(),
                         self.create_fake_trip_to_sf()]
        for new_trip in new_trip_list:
            confirmed_dest = {
                'metadata': {'key': 'manual/destination_confirm',
                             "write_ts": new_trip.metadata.write_ts,
                             "type": "message"
                            },
                "data": {
                    "start_ts": new_trip.data.start_ts,
                    "end_ts": new_trip.data.end_ts,
                    "label": "subway-san-francisco-18"
                }
            }
            enau.sync_phone_to_server(self.testUserId, [confirmed_dest])

        # The older trip (to PA) will generate a match 
        ret_sugg = sugg.calculate_yelp_server_suggestion_nominatim(self.testUserId)

        # Not sure what the expected behavior is.
        # should we return None or an invalid suggestion ("Unable to retrieve
        # datapoint...)
        # Returning None for now since that is what prior implementation did
        # Should be easy to change later
        self.assertIsNone(ret_sugg)

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
