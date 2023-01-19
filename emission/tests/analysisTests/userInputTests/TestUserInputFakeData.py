import unittest
import logging
import json
import bson.json_util as bju
import argparse
import numpy as np

import emission.core.wrapper.entry as ecwe
import emission.analysis.userinput.matcher as eaum
import emission.storage.decorations.trip_queries as esdt

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
            ecwe.Entry({"match_id": "foo", "metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<foo></foo>", "status": "ACTIVE"}}),
            ecwe.Entry({"match_id": "bar", "metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<bar></bar>", "status": "ACTIVE"}}),
            ecwe.Entry({"match_id": "foo", "metadata": {"key": "manual/trip_addition_input"}, "data": {"status": "DELETED"}})
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
            ecwe.Entry({"match_id": "foo", "metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<foo></foo>", "status": "ACTIVE"}}),
            ecwe.Entry({"match_id": "bar", "metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<bar></bar>", "status": "ACTIVE"}}),
            ecwe.Entry({"match_id": "foo", "metadata": {"key": "manual/trip_addition_input"}, "data": {"status": "DELETED"}}),
            ecwe.Entry({"match_id": "bar", "metadata": {"key": "manual/trip_addition_input"}, "data": {"status": "DELETED"}})
        ]
        for fm in fake_matches:
            eaum.handle_multi_non_deleted_match(fake_ct, fm)
        # Add two, delete two, we end up with none
        self.assertEqual(len(fake_ct["data"]["trip_addition"]), 0)
        # and it should be the bar entry
        self.assertEqual(fake_ct["data"]["trip_addition"], [])

        # Add none, delete two existing
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/confirmed_trip"}, "data": {"user_input": {}, "trip_addition": [
                {"match_id": "foo", "metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<foo></foo>", "status": "ACTIVE"}},
                {"match_id": "bar", "metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<bar></bar>", "status": "ACTIVE"}}
        ]}})
        fake_matches = [
            ecwe.Entry({"match_id": "foo", "metadata": {"key": "manual/trip_addition_input"}, "data": {"status": "DELETED"}}),
            ecwe.Entry({"match_id": "bar", "metadata": {"key": "manual/trip_addition_input"}, "data": {"status": "DELETED"}})
        ]
        for fm in fake_matches:
            eaum.handle_multi_non_deleted_match(fake_ct, fm)
        # Existing two, delete two, we end up with none
        self.assertEqual(len(fake_ct["data"]["trip_addition"]), 0)
        # and it should be the bar entry
        self.assertEqual(fake_ct["data"]["trip_addition"], [])

        # Add none, delete non-existing
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/confirmed_trip"}, "data": {"user_input": {}, "trip_addition": [
                {"match_id": "foo", "metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<foo></foo>", "status": "ACTIVE"}}
        ]}})
        fake_matches = [
            ecwe.Entry({"match_id": "bar", "metadata": {"key": "manual/trip_addition_input"}, "data": {"status": "DELETED"}})
        ]
        for fm in fake_matches:
            eaum.handle_multi_non_deleted_match(fake_ct, fm)
        # Existing 1, delete non-existent, we end up with one
        self.assertEqual(len(fake_ct["data"]["trip_addition"]), 1)
        # and it should be the bar entry
        self.assertEqual(fake_ct["data"]["trip_addition"], [{"match_id": "foo", "metadata": {"key": "manual/trip_addition_input"}, "data": {"xmlResponse": "<foo></foo>", "status": "ACTIVE"}}])

    def testFinalCandidate(self):
        # define some filter functions
        always_accept_fn = lambda pc: True
        always_reject_fn = lambda pc: False
        keep_start_lt_10 = lambda pc: pc.data.start_ts < 10

        # single entry multilabel
        single_entry_multilabel = {"metadata": {"key": "manual/mode_confirm", "write_ts": 1, "write_fmt_time": "1"},
            "data": {"start_ts": 8, "label": "foo"}}
        self.assertEqual(esdt.final_candidate(always_accept_fn, [single_entry_multilabel]), single_entry_multilabel)
        self.assertEqual(esdt.final_candidate(always_reject_fn, [single_entry_multilabel]), None)
        self.assertEqual(esdt.final_candidate(keep_start_lt_10, [single_entry_multilabel]), single_entry_multilabel)
        single_entry_multilabel["data"]["start_ts"] = 15
        self.assertEqual(esdt.final_candidate(keep_start_lt_10, [single_entry_multilabel]), None)

        # multi entry multilabel
        multi_entry_multilabel = [
            {"metadata": {"key": "manual/mode_confirm", "write_ts": 1, "write_fmt_time": "1"},
                "data": {"start_ts": 8, "label": "foo"}},
            {"metadata": {"key": "manual/mode_confirm", "write_ts": 2, "write_fmt_time": "2"},
                "data": {"start_ts": 8, "label": "foo"}},
            {"metadata": {"key": "manual/mode_confirm", "write_ts": 3, "write_fmt_time": "3"},
                "data": {"start_ts": 8, "label": "foo"}}]

        self.assertEqual(esdt.final_candidate(always_accept_fn, multi_entry_multilabel), multi_entry_multilabel[2])
        self.assertEqual(esdt.final_candidate(always_reject_fn, multi_entry_multilabel), None)
        self.assertEqual(esdt.final_candidate(keep_start_lt_10, multi_entry_multilabel), multi_entry_multilabel[2])
        multi_entry_multilabel[2]["data"]["start_ts"] = 15
        self.assertEqual(esdt.final_candidate(keep_start_lt_10, multi_entry_multilabel), multi_entry_multilabel[1])


        # multi entry survey
        multi_entry_multilabel = [
            {"metadata": {"key": "manual/trip_user_input", "write_ts": 1, "write_fmt_time": "1"},
                "data": {"xmlResponse": "<foo></foo>", "start_ts": 8, "start_fmt_time": 8}},
            {"metadata": {"key": "manual/trip_user_input", "write_ts": 2, "write_fmt_time": "2"},
                "data": {"xmlResponse": "<bar></bar>", "start_ts": 8, "start_fmt_time": 8}},
            {"metadata": {"key": "manual/trip_user_input", "write_ts": 3, "write_fmt_time": "3"},
                "data": {"xmlResponse": "<jar></jar>", "start_ts": 8, "start_fmt_time": 8}}]

        self.assertEqual(esdt.final_candidate(always_accept_fn, multi_entry_multilabel), multi_entry_multilabel[2])
        self.assertEqual(esdt.final_candidate(always_reject_fn, multi_entry_multilabel), None)
        self.assertEqual(esdt.final_candidate(keep_start_lt_10, multi_entry_multilabel), multi_entry_multilabel[2])
        multi_entry_multilabel[2]["data"]["start_ts"] = 15
        self.assertEqual(esdt.final_candidate(keep_start_lt_10, multi_entry_multilabel), multi_entry_multilabel[1])

    def testGetNotDeletedCandidates(self):
        # define some filter functions
        always_accept_fn = lambda pc: True
        always_reject_fn = lambda pc: False
        keep_start_lt_10 = lambda pc: pc.data.start_ts < 10

        # single entry multilabel
        single_entry_multilabel = {"match_id": "foo", "metadata": {"key": "manual/trip_addition_input", "write_ts": 1, "write_fmt_time": "1"},
            "data": {"start_ts": 8, "xmlResponse": "<foo></foo>", "start_fmt_time": 8, "status": "ACTIVE"}}
        self.assertEqual(esdt.get_not_deleted_candidates(always_accept_fn, [single_entry_multilabel]), [single_entry_multilabel])
        self.assertEqual(esdt.get_not_deleted_candidates(always_reject_fn, [single_entry_multilabel]), [])
        self.assertEqual(esdt.get_not_deleted_candidates(keep_start_lt_10, [single_entry_multilabel]), [single_entry_multilabel])
        single_entry_multilabel["data"]["start_ts"] = 15
        self.assertEqual(esdt.get_not_deleted_candidates(keep_start_lt_10, [single_entry_multilabel]), [])

        # multi entry all active
        multi_entry_multilabel = [
            {"match_id": "foo", "metadata": {"key": "manual/trip_addition_input", "write_ts": 1, "write_fmt_time": "1"},
                "data": {"start_ts": 8, "xmlResponse": "<foo></foo>", "start_fmt_time": 8, "status": "ACTIVE"}},
            {"match_id": "bar", "metadata": {"key": "manual/trip_addition_input", "write_ts": 2, "write_fmt_time": "2"},
                "data": {"start_ts": 8, "xmlResponse": "<foo></foo>", "start_fmt_time": 8, "status": "ACTIVE"}},
            {"match_id": "baz", "metadata": {"key": "manual/trip_addition_input", "write_ts": 3, "write_fmt_time": "3"},
                "data": {"start_ts": 8, "xmlResponse": "<foo></foo>", "start_fmt_time": 8, "status": "ACTIVE"}}]

        self.assertEqual(esdt.get_not_deleted_candidates(always_accept_fn, multi_entry_multilabel), multi_entry_multilabel)
        self.assertEqual(esdt.get_not_deleted_candidates(always_reject_fn, multi_entry_multilabel), [])
        self.assertEqual(esdt.get_not_deleted_candidates(keep_start_lt_10, multi_entry_multilabel), multi_entry_multilabel)
        multi_entry_multilabel[2]["data"]["start_ts"] = 15
        self.assertEqual(esdt.get_not_deleted_candidates(keep_start_lt_10, multi_entry_multilabel), multi_entry_multilabel[0:2])

        # multi entry one not deleted
        multi_entry_multilabel = [
            {"match_id": "foo", "metadata": {"key": "manual/trip_addition_input", "write_ts": 1, "write_fmt_time": "1"},
                "data": {"xmlResponse": "<foo></foo>", "start_ts": 8, "start_fmt_time": 8, "status": "ACTIVE"}},
            {"match_id": "foo", "metadata": {"key": "manual/trip_addition_input", "write_ts": 2, "write_fmt_time": "2"},
                "data": {"xmlResponse": "<bar></bar>", "start_ts": 8, "start_fmt_time": 8, "status": "DELETED"}},
            {"match_id": "bar", "metadata": {"key": "manual/trip_addition_input", "write_ts": 3, "write_fmt_time": "3"},
                "data": {"xmlResponse": "<jar></jar>", "start_ts": 8, "start_fmt_time": 8, "status": "ACTIVE"}}]

        self.assertEqual(esdt.get_not_deleted_candidates(always_accept_fn, multi_entry_multilabel), [multi_entry_multilabel[2]])
        self.assertEqual(esdt.get_not_deleted_candidates(always_reject_fn, multi_entry_multilabel), [])
        self.assertEqual(esdt.get_not_deleted_candidates(keep_start_lt_10, multi_entry_multilabel), [multi_entry_multilabel[2]])
        multi_entry_multilabel[2]["data"]["start_ts"] = 15
        self.assertEqual(esdt.get_not_deleted_candidates(keep_start_lt_10, multi_entry_multilabel), [])

        # multi entry all deleted
        # note that with the current implementation, if we have one deleted entry with id "foo"
        # we delete *all* matching ACTIVE entries, even if there are more than one
        # this is consistent in both testHandleMultiNonDeletedMatch and in this function
        multi_entry_multilabel = [
            {"match_id": "foo", "metadata": {"key": "manual/trip_addition_input", "write_ts": 1, "write_fmt_time": "1"},
                "data": {"xmlResponse": "<foo></foo>", "start_ts": 8, "start_fmt_time": 8, "status": "ACTIVE"}},
            {"match_id": "foo", "metadata": {"key": "manual/trip_addition_input", "write_ts": 2, "write_fmt_time": "2"},
                "data": {"xmlResponse": "<bar></bar>", "start_ts": 8, "start_fmt_time": 8, "status": "DELETED"}},
            {"match_id": "foo", "metadata": {"key": "manual/trip_addition_input", "write_ts": 3, "write_fmt_time": "3"},
                "data": {"xmlResponse": "<jar></jar>", "start_ts": 8, "start_fmt_time": 8, "status": "ACTIVE"}}]

        self.assertEqual(esdt.get_not_deleted_candidates(always_accept_fn, multi_entry_multilabel), [])
        self.assertEqual(esdt.get_not_deleted_candidates(always_reject_fn, multi_entry_multilabel), [])
        self.assertEqual(esdt.get_not_deleted_candidates(keep_start_lt_10, multi_entry_multilabel), [])
        multi_entry_multilabel[2]["data"]["start_ts"] = 15
        self.assertEqual(esdt.get_not_deleted_candidates(keep_start_lt_10, multi_entry_multilabel), [])

        # multi entry only DELETED entries
        multi_entry_multilabel = [
            {"match_id": "foo", "metadata": {"key": "manual/trip_addition_input", "write_ts": 2, "write_fmt_time": "2"},
                "data": {"xmlResponse": "<bar></bar>", "start_ts": 8, "start_fmt_time": 8, "status": "DELETED"}},
            {"match_id": "foo", "metadata": {"key": "manual/trip_addition_input", "write_ts": 3, "write_fmt_time": "3"},
                "data": {"xmlResponse": "<jar></jar>", "start_ts": 8, "start_fmt_time": 8, "status": "DELETED"}}]

        self.assertEqual(esdt.get_not_deleted_candidates(always_accept_fn, multi_entry_multilabel), [])
        self.assertEqual(esdt.get_not_deleted_candidates(always_reject_fn, multi_entry_multilabel), [])
        self.assertEqual(esdt.get_not_deleted_candidates(keep_start_lt_10, multi_entry_multilabel), [])
        multi_entry_multilabel[1]["data"]["start_ts"] = 15
        self.assertEqual(esdt.get_not_deleted_candidates(keep_start_lt_10, multi_entry_multilabel), [])

    def testGetAdditionsForTripObjects(self):
        # We already have an exhaustive set of tests for various matching options.
        # but we had some syntax errors in the additionsForTrip code
        # so let's add a few calls to ensure that it doesn't break
        import emission.storage.timeseries.abstract_timeseries as esta

        # Single add match
        self.testUUID = "INVALID_DUMMY"
        fake_ct = ecwe.Entry({"metadata": {"key": "analysis/confirmed_trip"},
            "data": {"user_input": {}, "start_ts": 5, "end_ts": 15,
                "start_fmt_time": 5, "end_fmt_time": 15}})
        try:
            ts = esta.TimeSeries.get_time_series(self.testUUID)
            fake_match = {"match_id": "foo","user_id": self.testUUID,
                "metadata": {"key": "manual/trip_addition_input", "write_ts": 1, "write_fmt_time": "1", "time_zone": "UTC"},
                "data": {"start_ts": 8, "end_ts": 10,
                    "label": "answered", "xmlResponse": "<foo></foo>",
                    "start_fmt_time": 8, "status": "ACTIVE"}
                }
            ts.insert(fake_match)
            # Now that we have a `match_id` instead of `_id`, the database inserts an `_id`
            # we need to remove it before checking equality because otherwise the extra field
            # will cause the check to fail
            addition_list = esdt.get_additions_for_trip_object(ts, fake_ct)
            del addition_list[0]["_id"]
            self.assertEqual(addition_list, [fake_match])
        finally:
            import emission.core.get_database as edb
            edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
       
        ## Note that another issue with using `_id` instead of `match_id` is that
        ## we cannot insert two entries (e.g. one ACTIVE and one DELETED) with the same id into the database.
        ## so for now, the second and third insert fail in this case, and the result is the first entry
        try:
            fake_matches = [
                {"match_id": "foo","user_id": self.testUUID,
                "metadata": {"key": "manual/trip_addition_input", "write_ts": 1, "write_fmt_time": "1", "time_zone": "UTC"},
                "data": {"start_ts": 8, "end_ts": 10,
                    "label": "answered", "xmlResponse": "<foo></foo>",
                    "start_fmt_time": 8, "status": "ACTIVE"}
                }, {"match_id": "foo","user_id": self.testUUID,
                "metadata": {"key": "manual/trip_addition_input", "write_ts": 1, "write_fmt_time": "1", "time_zone": "UTC"},
                "data": {"start_ts": 8, "end_ts": 10,
                    "label": "answered", "xmlResponse": "<foo></foo>",
                    "start_fmt_time": 8, "status": "DELETED"}
                }, {"match_id": "bar","user_id": self.testUUID,
                "metadata": {"key": "manual/trip_addition_input", "write_ts": 1, "write_fmt_time": "1", "time_zone": "UTC"},
                "data": {"start_ts": 9, "end_ts": 12,
                    "label": "answered", "xmlResponse": "<bar></bar>",
                    "start_fmt_time": 8, "status": "ACTIVE"}
                }
            ]
            ts.bulk_insert([ecwe.Entry(e) for e in fake_matches])
            addition_list = esdt.get_additions_for_trip_object(ts, fake_ct)
            # Now that we have a `match_id` instead of `_id`, the database inserts an `_id`
            # we need to remove it before checking equality because otherwise the extra field
            # will cause the check to fail
            del addition_list[0]["_id"]
            self.assertEqual(addition_list, [fake_matches[2]])
        finally:
            import emission.core.get_database as edb
            edb.get_timeseries_db().delete_many({"user_id": self.testUUID})

if __name__ == '__main__':
    etc.configLogging()

    parser = argparse.ArgumentParser()
    parser.add_argument("--algo_change",
        help="modifications to the algorithm", action="store_true")
    unittest.main()
