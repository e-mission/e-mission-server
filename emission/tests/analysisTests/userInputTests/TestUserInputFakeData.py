import unittest
import logging
import json
import bson.json_util as bju
import argparse
import numpy as np

import emission.core.wrapper.entry as ecwe
import emission.analysis.userinput.matcher as eaum

# Test imports
import emission.tests.common as etc

class TestUserInputFakeData(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    ### This is a test which tests the expected behavior of the list class that the
    # `del_entries_with_index` function uses
    # We're trying to remove all entries matching 'b'
    # we don't use remove directly because we want to use a lambda function to find matches by 'id'
    # instead of matching all values
    def testDirectFindRemoveEntries(self):
        # Approach 1: finding indices and then deleting them, does not work
        self.testUUID = "INVALID_DUMMY"
        test_arr = ['a', 'b', 'c', 'b', 'c']
        indices = [i for i, v in enumerate(test_arr) if v == 'b']
        self.assertEqual(indices, [1, 3])
        with self.assertRaisesRegex(TypeError, "list indices must be integers or slices, not list"):
            del test_arr[indices]
        del test_arr[indices[0]]
        self.assertEqual(test_arr, ['a', 'c', 'b', 'c'])
        del test_arr[indices[1]]
        self.assertEqual(test_arr, ['a', 'c', 'b'])
        self.assertTrue('b' in test_arr)

        # Approach 2: creating a new list through filtering does work
        test_arr = ['a', 'b', 'c', 'b', 'c']
        no_b_arr = [v for v in test_arr if v != 'b']
        self.assertNotIn("b", no_b_arr)
        self.assertEqual(no_b_arr, ["a", "c", "c"])

        # array addition manipulation
        test_arr = []
        test_arr.extend({"m": "a", "d": "b"})
        self.assertEqual(test_arr, ["m", "d"])
        test_arr = []
        test_arr.append({"m": "a", "d": "b"})
        self.assertEqual(test_arr, [{"m": "a", "d": "b"}])
        test_arr.append({"m": "z", "d": "y"})
        self.assertEqual(test_arr, [{"m": "a", "d": "b"}, {"m": "z", "d": "y"}])

    # This is a unit test 
    def testHandleSingleMostRecentMatch(self):
        # Single match for one multi-label
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/confirmed_trip"}, "data": {"user_input": {}}})
        fake_match = ecwe.Entry({"metadata": {"key": "manual/mode_confirm"}, "data": {"label": "FOO"}})
        eaum.handle_single_most_recent_match(fake_ct, fake_match)
        self.assertEqual(fake_ct["data"]["user_input"], {"mode_confirm": "FOO"})

        # Single match for two multi-labels
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/confirmed_trip"}, "data": {"user_input": {}}})
        fake_matches = [
            ecwe.Entry({"metadata": {"key": "manual/mode_confirm"}, "data": {"label": "FOO"}}),
            ecwe.Entry({"metadata": {"key": "manual/purpose_confirm"}, "data": {"label": "BAR"}})
        ]
        for fm in fake_matches:
            eaum.handle_single_most_recent_match(fake_ct, fm)
        self.assertEqual(fake_ct["data"]["user_input"], {"mode_confirm": "FOO", "purpose_confirm": "BAR"})

        # Multiple matches for two multi-labels
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/confirmed_trip"}, "data": {"user_input": {}}})
        fake_matches = [
            ecwe.Entry({"metadata": {"key": "manual/mode_confirm"}, "data": {"label": "FOO"}}),
            ecwe.Entry({"metadata": {"key": "manual/purpose_confirm"}, "data": {"label": "BAR"}}),
            ecwe.Entry({"metadata": {"key": "manual/mode_confirm"}, "data": {"label": "GOO"}}),
            ecwe.Entry({"metadata": {"key": "manual/purpose_confirm"}, "data": {"label": "JAR"}})
        ]
        for fm in fake_matches:
            eaum.handle_single_most_recent_match(fake_ct, fm)
        self.assertEqual(fake_ct["data"]["user_input"], {"mode_confirm": "GOO", "purpose_confirm": "JAR"})

        # Single match for a trip user input
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/confirmed_trip"}, "data": {"user_input": {}}})
        fake_matches = [
            ecwe.Entry({"metadata": {"key": "manual/trip_user_input"}, "data": {"label": "2 modes, 2 purposes"}})
        ]
        for fm in fake_matches:
            eaum.handle_single_most_recent_match(fake_ct, fm)
        self.assertEqual(fake_ct["data"]["user_input"], {"trip_user_input": fake_matches[0]})

        # multiple matches for a trip user input
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/confirmed_trip"}, "data": {"user_input": {}}})
        fake_matches = [
            ecwe.Entry({"metadata": {"key": "manual/trip_user_input"}, "data": {"label": "2 modes, 2 purposes"}}),
            ecwe.Entry({"metadata": {"key": "manual/trip_user_input"}, "data": {"label": "5 modes, 5 purposes"}})
        ]
        for fm in fake_matches:
            eaum.handle_single_most_recent_match(fake_ct, fm)
        self.assertEqual(fake_ct["data"]["user_input"], {"trip_user_input": fake_matches[1]})

    def testHandleMultiNonDeletedMatch(self):
        # Single add match
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/confirmed_trip"}, "data": {"user_input": {}}})
        fake_match = ecwe.Entry({"metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<foo></foo>", "status": "ACTIVE"}})
        eaum.handle_multi_non_deleted_match(fake_ct, fake_match)
        self.assertEqual(len(fake_ct["data"]["trip_addition"]), 1)
        self.assertEqual(fake_ct["data"]["trip_addition"], [fake_match])

        # Two add matches
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/confirmed_trip"}, "data": {"user_input": {}}})
        fake_matches = [
            ecwe.Entry({"metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<foo></foo>", "status": "ACTIVE"}}),
            ecwe.Entry({"metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<bar></bar>", "status": "ACTIVE"}})
        ]
        for fm in fake_matches:
            eaum.handle_multi_non_deleted_match(fake_ct, fm)
        self.assertEqual(len(fake_ct["data"]["trip_addition"]), 2)
        self.assertEqual(fake_ct["data"]["trip_addition"], fake_matches)
    
        # Add two, delete one
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/confirmed_trip"}, "data": {"user_input": {}}})
        fake_matches = [
            ecwe.Entry({"_id": "foo", "metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<foo></foo>", "status": "ACTIVE"}}),
            ecwe.Entry({"_id": "bar", "metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<bar></bar>", "status": "ACTIVE"}}),
            ecwe.Entry({"_id": "foo", "metadata": {"key": "manual/trip_addition_input"}, "data": {"status": "DELETED"}})
        ]
        for fm in fake_matches:
            eaum.handle_multi_non_deleted_match(fake_ct, fm)
        # Add two, delete 1, we end up with one
        self.assertEqual(len(fake_ct["data"]["trip_addition"]), 1)
        # and it should be the bar entry
        self.assertEqual(fake_ct["data"]["trip_addition"], [fake_matches[1]])

        # Add two, delete two
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/confirmed_trip"}, "data": {"user_input": {}}})
        fake_matches = [
            ecwe.Entry({"_id": "foo", "metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<foo></foo>", "status": "ACTIVE"}}),
            ecwe.Entry({"_id": "bar", "metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<bar></bar>", "status": "ACTIVE"}}),
            ecwe.Entry({"_id": "foo", "metadata": {"key": "manual/trip_addition_input"}, "data": {"status": "DELETED"}}),
            ecwe.Entry({"_id": "bar", "metadata": {"key": "manual/trip_addition_input"}, "data": {"status": "DELETED"}})
        ]
        for fm in fake_matches:
            eaum.handle_multi_non_deleted_match(fake_ct, fm)
        # Add two, delete two, we end up with none
        self.assertEqual(len(fake_ct["data"]["trip_addition"]), 0)
        # and it should be the bar entry
        self.assertEqual(fake_ct["data"]["trip_addition"], [])

        # Add none, delete two existing
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/confirmed_trip"}, "data": {"user_input": {}, "trip_addition": [
                {"_id": "foo", "metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<foo></foo>", "status": "ACTIVE"}},
                {"_id": "bar", "metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<bar></bar>", "status": "ACTIVE"}}
        ]}})
        fake_matches = [
            ecwe.Entry({"_id": "foo", "metadata": {"key": "manual/trip_addition_input"}, "data": {"status": "DELETED"}}),
            ecwe.Entry({"_id": "bar", "metadata": {"key": "manual/trip_addition_input"}, "data": {"status": "DELETED"}})
        ]
        for fm in fake_matches:
            eaum.handle_multi_non_deleted_match(fake_ct, fm)
        # Existing two, delete two, we end up with none
        self.assertEqual(len(fake_ct["data"]["trip_addition"]), 0)
        # and it should be the bar entry
        self.assertEqual(fake_ct["data"]["trip_addition"], [])

        # Add none, delete non-existing
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/confirmed_trip"}, "data": {"user_input": {}, "trip_addition": [
                {"_id": "foo", "metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<foo></foo>", "status": "ACTIVE"}}
        ]}})
        fake_matches = [
            ecwe.Entry({"_id": "bar", "metadata": {"key": "manual/trip_addition_input"}, "data": {"status": "DELETED"}})
        ]
        for fm in fake_matches:
            eaum.handle_multi_non_deleted_match(fake_ct, fm)
        # Existing 1, delete non-existent, we end up with one
        self.assertEqual(len(fake_ct["data"]["trip_addition"]), 1)
        # and it should be the bar entry
        self.assertEqual(fake_ct["data"]["trip_addition"], [{"_id": "foo", "metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<foo></foo>", "status": "ACTIVE"}}])

if __name__ == '__main__':
    etc.configLogging()

    parser = argparse.ArgumentParser()
    parser.add_argument("--algo_change",
        help="modifications to the algorithm", action="store_true")
    unittest.main()
