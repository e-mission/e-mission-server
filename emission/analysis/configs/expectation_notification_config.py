import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import arrow

import emission.analysis.config as eac

# These may be altered to run tests
_test_options = {
    "use_sample": False,
    "override_keylist": None
}

# PRIVATE HELPER FUNCTIONS
def _get_expectation_config():
    if _test_options["use_sample"]:
        config_file = open('conf/ux/expectations.conf.json.sample')
    else:
        try:
            config_file = open('conf/ux/expectations.conf.json')
        except FileNotFoundError:
            print("expectations.conf.json not configured, falling back to sample, default configuration")
            config_file = open('conf/ux/expectations.conf.json.sample')
    config_data = json.load(config_file)
    config_file.close()
    # TODO: validate JSON against schema
    return config_data

def _get_keylist():
    if _test_options["override_keylist"] is not None: return _test_options["override_keylist"]
    keylist = eac.get_config()["userinput.keylist"]
    assert len(keylist) > 0
    keylist = [s.split("/")[1] for s in keylist]  # Turns e.g. "manual/mode_confirm" into "mode_confirm"
    return keylist

_config = _get_expectation_config()
_keylist = _get_keylist()

def _get_done_rule():
    return {
        "trigger": 3,
        "expect": {"type": "none"},
        "notify": {"type": "none"}
    }

# The time from the config file may or may not have time zone information attached.
# This is by design, so that times may be specified either absolutely or in the user's (phone's) time zone.
# We don't know the user's time zone when writing the config file, but we do now, so we populate it here.
# If there's already time zone information, we convert the time instead of just replacing the time zone info and keeping the numbers the same.
def _parse_datetime_maybe_naive(dtstr, tz):
    dt = datetime.fromisoformat(dtstr)
    naive = dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None
    ar = arrow.get(dt)
    return ar.replace(tzinfo=tz) if naive else ar.to(tz)

# Get the date a schedule most recently became active before (or exactly at) trip_date
# Arrow does not seem to have an equivalent of moment.js's diff(), so we have to get our hands a bit dirty here.
# TODO: Surely there's some function somewhere I'm missing?
def _soonest_start_before(schedule, trip_date):
    first_start = _parse_datetime_maybe_naive(schedule["startDate"], trip_date.tzinfo)  # Now everything is in the same time zone
    if trip_date < first_start: return None  # Don't go back in time
    recurrence_value = schedule["recurrenceValue"]
    # Add the largest number of recurrences to first_start as we can without exceeding trip_date to obtain the date that the mode most recently became active.
    return {
        "months": lambda d1,d2: (lambda d: d1.shift(years=d.years, months=d.months//recurrence_value*recurrence_value))(relativedelta(d2.datetime, d1.datetime)),
        "weeks":  lambda d1,d2: d1.shift(weeks=(d2-d1).days//7//recurrence_value*recurrence_value),
        "days":   lambda d1,d2: d1.shift(days=(d2-d1).days//recurrence_value*recurrence_value)
    }[schedule["recurrenceUnit"]](first_start, trip_date)

# Determines whether the given trip_date falls under the schedule of the given mode. Relies on trip_date having the local time zone.
def _mode_matches_date(mode, trip_date):
    if "schedule" not in mode: return True  # If the mode has no schedule, it matches all dates
    soonest_start = _soonest_start_before(mode["schedule"], trip_date)
    if soonest_start is None: return False  # If it's before the start date, it doesn't match
    # Mode is active if it's been less than duration days since soonestStart
    return (trip_date - soonest_start) / timedelta(days=1) < mode["schedule"]["duration"]

def _get_collection_mode_by_schedule(trip):
    trip_tz = trip["data"]["end_local_dt"]["timezone"]
    trip_date = arrow.get(trip["data"]["end_ts"]).to(trip_tz)
    for mode in _config["modes"]:
        if _mode_matches_date(mode, trip_date): return mode
    raise ValueError("Trip date does not match any modes; this means the config file lacks a schedule-less mode")

def _rule_matches_label(mode, rule, trip, label):
    # If we were doing this on the phone, we would use finalInference.
    # Here, we just take the most likely prediction.
    if len(trip["data"]["inferred_labels"]) == 0:
        return rule["trigger"] == -1
    prediction = max(trip["data"]["inferred_labels"], key = lambda d: d["p"])
    confidence = prediction["p"]
    if confidence <= mode["confidenceThreshold"] or label not in prediction["labels"]:
        return rule["trigger"] == -1
    return confidence <= rule["trigger"]

def _get_rule_for_label(mode, trip, label):
    rules = []
    for rule in mode["rules"]:
        if _rule_matches_label(mode, rule, trip, label): rules.append(rule)
    return None if len(rules) == 0 else min(rules, key = lambda r: r["trigger"])

def _get_rule(trip):
    if len(_keylist) == 0: return _get_done_rule()
    mode = _get_collection_mode_by_schedule(trip)
    rules = []
    for input in _keylist:
        rule = _get_rule_for_label(mode, trip, input)
        if rule is not None: rules.append(rule)
    if len(rules) == 0:
        raise ValueError("Trip does not match any rules; this means the trip is malformed or the active collection mode is missing required rules")
        # return _get_done_rule()
    return min(rules, key = lambda r: r["trigger"])


# PUBLIC INTERFACE
def reload_config():
    global _config, _keylist
    _config = _get_expectation_config()
    _keylist = _get_keylist()

def get_collection_mode_label(trip):
    return _get_collection_mode_by_schedule(trip)["label"]

def get_confidence_threshold(trip):
    return _get_collection_mode_by_schedule(trip)["confidenceThreshold"]

def get_expectation(trip):
    return _get_rule(trip)["expect"]

def get_notification(trip):
    return _get_rule(trip)["notify"]
