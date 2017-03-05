# Input spec sample at
# emission/net/ext_service/push/sample.specs/trip_metrics.query.sample sample
# finds all users who have at least one day in Feb 2017 with no more than 10
# walk sections and a walk distance of at least 1km during the evening commute
# hours

# Input: query spec
# Output: list of uuids
# 
import logging
import numpy as np

import emission.core.wrapper.motionactivity as ecwm

import emission.net.api.metrics as enam

import emission.storage.decorations.local_date_queries as esdl
import emission.storage.decorations.location_queries as esdlq
import emission.storage.timeseries.geoquery as estg
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.tcquery as esttc
import emission.storage.decorations.analysis_timeseries_queries as esda

def get_metric_list(checks):
  metric_list = [e["metric"] for e in checks]
  logging.debug("Returning %s" % metric_list)
  return metric_list

def compare_value(threshold, summed_value):
  if '$gt' in threshold:
    return summed_value > threshold['$gt']
  if '$gte' in threshold:
    return summed_value >= threshold['$gte']
  if '$lt' in threshold:
    return summed_value < threshold['$lt']
  if '$lte' in threshold:
    return summed_value <= threshold['$lte']
  return False

def matches_check(check, msts):
  # We know that the metric in the check matches the result because that's the
  # way that the metrics API works. So we just need to check mode versus threshold
  # entry looks like this (for count)
  # ModeStatTimeSummary({'fmt_time': '2017-01-20T00:00:00+00:00',
  # 'nUsers': 1,
  # 'UNKNOWN': 1,
  # 'ts': 1484870400,
  # 'AIR_OR_HSR': 2,
  # 'local_dt': LocalDate(...)})
  mode_list = check['modes']
  summed_value = 0
  for mode in mode_list:
     summed_value = summed_value + msts.get(mode, 0)
  return compare_value(check["threshold"], summed_value)

def is_matched_user(user_id, spec):
  metric_list = get_metric_list(spec["checks"])
  time_type = spec['time_type']
  if 'from_local_date' in spec and 'to_local_date' in spec:
    freq_metrics = enam.summarize_by_local_date(user_id,
        spec["from_local_date"], spec["to_local_date"],
        spec["freq"], metric_list, include_aggregate=False)
  elif 'start_time' in spec and 'end_time' in spec:
    freq_metrics = enam.summarize_by_timestamp(user_id,
        spec["start_time"], spec["end_time"],
        spec["freq"], metric_list, include_aggregate=False)
  else:
    # If no start and end times are specified, we assume that this is a
    # timestamp query because we can come up with a reasonable start and end
    # time for timestamps but not for local_dates, which are basically a filter.
    # so if we run this on the first of a month, for example, we won't find
    # anything, which seems bogus and not what people would expect
    assert time_type == "timestamp", "time_type = %s, expected timestamp" % time_type
    freq_metrics = enam.summarize_by_timestamp(user_id,
        0, time.time(), spec["freq"], metric_list, include_aggregate=False)

  assert(freq_metrics is not None)
  assert('user_metrics' in freq_metrics)
  curr_user_metrics = freq_metrics['user_metrics']
  checks = spec['checks']
  check_results = np.zeros(len(checks))
  for i, check in enumerate(checks):
    curr_metric_result = curr_user_metrics[i]
    # curr_freq_result is a list of ModeStatTimeSummary objects, one for each
    # grouped time interval in the range
    # e.g. for daily, 2017-01-19, 2017-01-20, 2017-01-21, 2017-01-22, 2017-01-23, ....
    
    for msts in curr_metric_result:
    # We defined our check as being true if it is true for _any_ grouped time
    # period in the range. So as long as we find a match for that check, we are
    # good!
      if matches_check(check, msts):
        check_results[i] = True

  logging.info("For user_id %s, check result array = %s, all? %s" % (user_id, check_results, np.all(check_results)))
  return np.all(check_results)

def query(spec):
  # Copied from `emission/pipeline/scheduler.py`
  # Refactor and pull out, possibly in user?
  import emission.core.get_database as edb

  all_uuids = [e["uuid"] for e in edb.get_uuid_db().find()] 
  # Add back the test phones for now so that we can test the data
  # collection changes before deploying them in the wild
  # sel_uuids.extend(TEMP_HANDLED_PUBLIC_PHONES)
  sel_uuids = all_uuids

  matched_uuid_list = [uuid for uuid in sel_uuids if is_matched_user(uuid, spec)]
  logging.info("matched matched_uuid_list of length = %s = %s" % 
    (len(matched_uuid_list), matched_uuid_list))
  return matched_uuid_list
