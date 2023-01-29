import unittest
import arrow
import copy

import emission.analysis.configs.expectation_notification_config as eace
import emission.tests.common as etc


# Exhaustive (and exhausting) tests of expectation_notification_config, including the behind-the-scenes rules algorithms and timey-wimey stuff
class TestExpectationNotificationConfig(unittest.TestCase):
    def setUp(self):
        self.test_options_stash = copy.copy(eace._test_options)
        eace._test_options = {
            "use_sample": True,
            "override_keylist": ["label1", "label2"]
        }
        eace.reload_config()

        self.tz = "Etc/GMT-8"
        # Note that these depend on certain values in expectations.conf.json.sample, as will other values later
        self.test_dates = {
            "before_intensive": arrow.get("2021-05-01T20:00:00.000", tzinfo=self.tz),  # Ensures schedules can't apply before their start_date
            "first_intensive":  arrow.get("2021-06-01T20:00:00.000", tzinfo=self.tz),  # Ensures that a given mode starts exactly at start_date
            "first_relaxed":    arrow.get("2021-06-08T20:00:00.000", tzinfo=self.tz),  # Ensures that a given mode ends exactly when it should
            "nth_intensive":    arrow.get("2021-09-04T20:00:00.000", tzinfo=self.tz),  # Ensures recurrence works as it should
            "mth_intensive":    arrow.get("2023-02-03T20:00:00.000", tzinfo=self.tz),  # Ensures recurrence works as it should over a long time
            "mth_relaxed":      arrow.get("2023-02-12T20:00:00.000", tzinfo=self.tz)   # Ensures recurrence is sufficiently selective
        }

        # For each of test_dates, construct a dict with the keys we care about and pretend it's a trip
        self.fake_trips = {
            label: {
                "data": {
                    "end_ts": testdate.timestamp,
                    "end_local_dt": {
                        "timezone": self.tz
                    }
                }
            } for label,testdate in self.test_dates.items()}


    def tearDown(self):
        eace._test_options = self.test_options_stash
        eace.reload_config()


    # Briefly make sure we're getting the test config data correctly
    def testLoading(self):
        mode_labels = [mode["label"] for mode in eace._config["modes"]]
        self.assertEqual(len(mode_labels), 2)
        self.assertEqual(set(mode_labels), {"intensive", "relaxed"})


    def testParseDatetimeMaybeNaive(self):
        # Etc/GMT+6 really means UTC-06:00 :(
        self.assertEqual(eace._parse_datetime_maybe_naive("2021-07-09T12:34:56.789", "Etc/GMT+6"), arrow.get("2021-07-09T12:34:56.789-06:00"))
        self.assertEqual(eace._parse_datetime_maybe_naive("2021-07-09T12:34:56.789+02:00", "Etc/GMT+6"), arrow.get("2021-07-09T12:34:56.789+02:00"))

    def testSoonestStartBefore(self):
        by_days = {"startDate": "2021-06-01T20:00:00.000", "recurrenceUnit": "days", "recurrenceValue": 5}
        by_weeks = {"startDate": "2021-06-01T20:00:00.000", "recurrenceUnit": "weeks", "recurrenceValue": 1}
        by_months = {"startDate": "2021-06-01T20:00:00.000", "recurrenceUnit": "months", "recurrenceValue": 1}

        past = arrow.get("2021-06-01T19:00:00.000", tzinfo=self.tz)
        present = arrow.get("2021-06-03T20:00:00.000", tzinfo=self.tz)
        future = arrow.get("2035-11-28T07:00:00.000", tzinfo=self.tz)

        self.assertEqual(eace._soonest_start_before(by_days, past), None)
        self.assertEqual(eace._soonest_start_before(by_weeks, past), None)
        self.assertEqual(eace._soonest_start_before(by_months, past), None)

        self.assertEqual(eace._soonest_start_before(by_days, present), arrow.get("2021-06-01T20:00:00.000", tzinfo=self.tz))
        self.assertEqual(eace._soonest_start_before(by_weeks, present), arrow.get("2021-06-01T20:00:00.000", tzinfo=self.tz))
        self.assertEqual(eace._soonest_start_before(by_months, present), arrow.get("2021-06-01T20:00:00.000", tzinfo=self.tz))

        self.assertEqual(eace._soonest_start_before(by_days, future), arrow.get("2035-11-25T20:00:00.000", tzinfo=self.tz))
        self.assertEqual(eace._soonest_start_before(by_weeks, future), arrow.get("2035-11-27T20:00:00.000", tzinfo=self.tz))
        self.assertEqual(eace._soonest_start_before(by_months, future), arrow.get("2035-11-01T20:00:00.000", tzinfo=self.tz))


    def testModeMatchesDate(self):
        mode_intensive = eace._config["modes"][0]
        mode_relaxed = eace._config["modes"][1]

        # Case 1: no schedule
        for dt in self.test_dates.values():
            self.assertTrue(eace._mode_matches_date(mode_relaxed, dt))

        # Case 2: yes schedule and it matches
        for testlabel in {"first_intensive", "nth_intensive", "mth_intensive"}:
            self.assertTrue(eace._mode_matches_date(mode_intensive, self.test_dates[testlabel]))
        
        # Case 3: yes schedule and it doesn't match
        for testlabel in {"before_intensive", "first_relaxed", "mth_relaxed"}:
            self.assertFalse(eace._mode_matches_date(mode_intensive, self.test_dates[testlabel]))


    def testGetCollectionModeBySchedule(self):
        answers = {
            "before_intensive": "relaxed",
            "first_intensive":  "intensive",
            "first_relaxed":    "relaxed",
            "nth_intensive":    "intensive",
            "mth_intensive":    "intensive",
            "mth_relaxed":      "relaxed"
        }

        for label,trip in self.fake_trips.items():
            self.assertEqual(eace._get_collection_mode_by_schedule(trip)["label"], answers[label])


    def testRuleMatchesLabel(self):
        # An answers entry is a (labels, list of indices of matching rules in intensive mode, same for relaxed mode) tuple
        answers = [
            ([{"labels": {"label2": "value8"}, "p": 1}],    [0],   [0]),  # label1 is not in here, so it should match the red rule when we test with label1 below
            ([{"labels": {"label1": "value1"}, "p": 1}],    [1],   [1]),
            ([{"labels": {"label1": "value2"}, "p": 0.96}], [1],   [1]),
            ([{"labels": {"label1": "value3"}, "p": 0.95}], [1],   [1,2]),
            ([{"labels": {"label1": "value4"}, "p": 0.75}], [1,2], [1,2,3]),
            ([{"labels": {"label1": "value5"}, "p": 0.65}], [0],   [1,2,3]),
            ([{"labels": {"label1": "value6"}, "p": 0.55}], [0],   [0]),
            ([{"labels": {"label1": "value7"}, "p": 0}],    [0],   [0])
        ]

        for labels,intensive_answer,relaxed_answer in answers:
            for i_mode,answer in [(0, intensive_answer), (1, relaxed_answer)]:
                mode = eace._config["modes"][i_mode]
                for i_rule,rule in enumerate(mode["rules"]):
                    rule = mode["rules"][i_rule]
                    trip = {"data": {"inferred_labels": labels}}
                    guess = eace._rule_matches_label(mode, rule, trip, "label1")
                    if i_rule in answer: self.assertTrue(guess)
                    else: self.assertFalse(guess)


    def testGetRuleForLabel(self):
        # An answers entry is a (labels, index of corresponding rule under intensive mode, index of corresponding rule under relaxed mode) tuple
        answers = [
            ([{"labels": {"label2": "value8"}, "p": 1}],    0, 0),
            ([{"labels": {"label1": "value1"}, "p": 1}],    1, 1),
            ([{"labels": {"label1": "value2"}, "p": 0.96}], 1, 1),
            ([{"labels": {"label1": "value3"}, "p": 0.95}], 1, 2),
            ([{"labels": {"label1": "value4"}, "p": 0.75}], 2, 3),
            ([{"labels": {"label1": "value5"}, "p": 0.65}], 0, 3),
            ([{"labels": {"label1": "value6"}, "p": 0.55}], 0, 0),
            ([{"labels": {"label1": "value7"}, "p": 0}],    0, 0)
        ]

        for labels,intensive_answer,relaxed_answer in answers:
            trip = {"data": {"inferred_labels": labels}}
            for i, mode in enumerate(eace._config["modes"]):
                guess = eace._get_rule_for_label(mode, trip, "label1")
                answer = mode["rules"][[intensive_answer, relaxed_answer][i]]
                self.assertEqual(guess, answer)


    def testGetRule(self):
        # No labels, so not possible to expect anything of the user
        keylist_stash = eace._keylist
        eace._keylist = []
        guess = eace._get_rule({})
        self.assertEqual(guess["expect"], {"type": "none"})
        self.assertEqual(guess["notify"], {"type": "none"})
        eace._keylist = keylist_stash

        # Normal case: intensive mode, at least one red label
        trip0 = copy.copy(self.fake_trips["first_intensive"])
        trip0["data"]["inferred_labels"] = [{"labels": {"label2": "value8"}, "p": 1}]
        self.assertEqual(eace._get_rule(trip0), eace._config["modes"][0]["rules"][0])

        # Normal case: relaxed mode, very confident yellow labels
        trip1 = copy.copy(self.fake_trips["before_intensive"])
        trip1["data"]["inferred_labels"] = [{"labels": {"label1": "value1", "label2": "value8"}, "p": 1}]
        self.assertEqual(eace._get_rule(trip1), eace._config["modes"][1]["rules"][1])

        # Malformed trip: p can't be above 1
        tripbad = copy.copy(self.fake_trips["before_intensive"])
        tripbad["data"]["inferred_labels"] = [{"labels": {"label1": "value1", "label2": "value8"}, "p": 10}]
        self.assertRaises(ValueError, lambda: eace._get_rule(tripbad))


    def testReloadConfig(self):
        keylist_stash = eace._keylist
        test_keylist = ["newkey"]
        eace._test_options["override_keylist"] = test_keylist
        eace.reload_config()
        self.assertEqual(eace._keylist, test_keylist)
        eace._keylist = keylist_stash


    # The per-trip public interface is pretty simple; we can test it all in one go
    def testInterface(self):
        trip = copy.copy(self.fake_trips["first_relaxed"])
        trip["data"]["inferred_labels"] = [{"labels": {"label1": "value1", "label2": "value8"}, "p": 0.90}]

        self.assertEqual(eace.get_collection_mode_label(trip), "relaxed")
        self.assertEqual(eace.get_confidence_threshold(trip), 0.55)
        self.assertEqual(eace.get_expectation(trip), {"type": "randomFraction", "value": 0.05})
        self.assertEqual(eace.get_notification(trip), {"type": "dayEnd"})

def main():
    etc.configLogging()
    unittest.main()

if __name__ == "__main__":
    main()
