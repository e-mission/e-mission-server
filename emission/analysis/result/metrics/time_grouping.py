from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import range
from builtins import *
import enum
import pandas as pd
import arrow as arrow
import logging

import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.decorations.local_date_queries as esdl

import emission.storage.timeseries.tcquery as esttc
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.aggregate_timeseries as estag
import emission.core.wrapper.motionactivity as ecwm
import emission.core.wrapper.modestattimesummary as ecwms
import emission.core.wrapper.modeprediction as ecwmp
import emission.core.wrapper.localdate as ecwl

import emission.analysis.config as eac

def group_by_timestamp(user_id, start_ts, end_ts, freq, summary_fn_list):
    """
    Get grouped dataframes for the specific time range and at the specified frequency
    :param user_id: The user for whom we are computing this information. None for all users.
    :param from_ld: The start timestamp
    :param to_ld: The end timestamp
    :param freq: The frequency as specified in a pandas date_range frequency string.
    We only support frequencies of a day or longer in order to return the data
    in a format that makes sense
    http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases
    The canonical list can be found at:
    > pandas.tseries.offsets.prefix_mapping
    :return: a dict containing the last start_ts of the last section processed
        and a result list of ModeStatTimeSummary objects
        If there were no matching sections, the last start_ts is None
        and the list is empty.
    """
    time_query = estt.TimeQuery("data.start_ts", start_ts, end_ts)
    section_df = esda.get_data_df(eac.get_section_key_for_analysis_results(),
                                  user_id=user_id, time_query=time_query,
                                  geo_query=None)
    if len(section_df) == 0:
        logging.info("Found no entries for user %s, time_query %s" % (user_id, time_query))
        return {
            "last_ts_processed": None,
            "result": [[] for i in range(len(summary_fn_list))]
        }
    logging.debug("first row is %s" % section_df.iloc[0])
    secs_to_nanos = lambda x: x * 10 ** 9
    section_df['start_dt'] = pd.to_datetime(secs_to_nanos(section_df.start_ts))
    time_grouped_df = section_df.groupby(pd.Grouper(freq=freq, key='start_dt'))
    return {
        "last_ts_processed": section_df.iloc[-1].start_ts,
        "result": [grouped_to_summary(time_grouped_df, timestamp_fill_times, summary_fn)
                   for summary_fn in summary_fn_list]
    }

def timestamp_fill_times(key, ignored, metric_summary):
    dt = arrow.get(key)
    metric_summary.ts = dt.timestamp
    metric_summary.local_dt = ecwl.LocalDate.get_local_date(dt.timestamp, 'UTC')
    metric_summary.fmt_time = dt.isoformat()

class LocalFreq(enum.Enum):
    DAILY = 0
    MONTHLY = 1
    YEARLY = 2

def group_by_local_date(user_id, from_dt, to_dt, freq, summary_fn_list):
    """
    Get grouped data frames for the specified local date range and frequency
    :param user_id: id for the user. None for aggregate.
    :param from_dt: start local dt object. We assume that only the year, month
    and date entries are filled in and represent a date range.
    :param to_dt: end local dt object. We assume that only the year, month
    and date entries are filled in and represent a date range.
    :param freq: since we only expand certain local_dt fields, we can only
    support frequencies corresponding to them. These are represented in the
    `LocalFreq` enum.
    :return: a dict containing the last start_ts of the last section processed
        and a result list of ModeStatTimeSummary objects
        If there were no matching sections, the last start_ts is None
        and the list is empty.
    """
    time_query = esttc.TimeComponentQuery("data.start_local_dt", from_dt, to_dt)
    section_df = esda.get_data_df(eac.get_section_key_for_analysis_results(),
                                  user_id=user_id, time_query=time_query,
                                  geo_query=None)
    if len(section_df) == 0:
        logging.info("Found no entries for user %s, time_query %s" % (user_id, time_query))
        return {
            "last_ts_processed": None,
            "result": [[] for i in range(len(summary_fn_list))]
        }

    groupby_arr = _get_local_group_by(freq)
    time_grouped_df = section_df.groupby(groupby_arr)
    local_dt_fill_fn = _get_local_key_to_fill_fn(freq)
    return {
        "last_ts_processed": section_df.iloc[-1].start_ts,
        "result": [grouped_to_summary(time_grouped_df, local_dt_fill_fn, summary_fn)
                        for summary_fn in summary_fn_list]
    }

# by default, the incoming values for the keys are `numpy.int64` for reasons
# that I don't understand. This breaks serialization
# (https://github.com/e-mission/e-mission-docs/issues/530)
# converting to regular ints to avoid this issue

def fix_int64_key_if_needed(key):
    if isinstance(key, tuple):
        logging.debug("Converting %d fields from int64 to regular integer" % len(key))
        # print("before conversion, types = %s" % str(tuple([type(k) for k in key])))
        mod_keys = tuple([int(k) for k in key])
        # print("after conversion, types = %s" % str(tuple([type(k) for k in key])))
        return mod_keys
    else:
        return key

def grouped_to_summary(time_grouped_df, key_to_fill_fn, summary_fn):
    ret_list = []
    # When we group by a time range, the key is the end of the range
    for key, section_group_df in time_grouped_df:
        curr_msts = ecwms.ModeStatTimeSummary()
        key = fix_int64_key_if_needed(key)
        key_to_fill_fn(key, section_group_df, curr_msts)
        curr_msts.nUsers = len(section_group_df.user_id.unique())
        mode_grouped_df = section_group_df.groupby('sensed_mode')
        mode_results = summary_fn(mode_grouped_df)
        for mode, result in mode_results.items():
            if eac.get_section_key_for_analysis_results() == "analysis/inferred_section":
                curr_msts[ecwmp.PredictedModeTypes(mode).name] = result
            else:
                curr_msts[ecwm.MotionTypes(mode).name] = result
        ret_list.append(curr_msts)
#         import bson.json_util as bju
#         logging.debug("After appending %s, ret_list = %s" % (curr_msts, ret_list))
#         for k in curr_msts.keys():
#             print("Serializing key = %s" % k)
#             logging.debug("Serializing key %s = %s" %
#                 (k, bju.dumps(curr_msts[k])))
    return ret_list

def _get_local_group_by(local_freq):
    if (local_freq == LocalFreq.DAILY):
        return ['start_local_dt_year', 'start_local_dt_month', 'start_local_dt_day']
    elif (local_freq == LocalFreq.MONTHLY):
        return ['start_local_dt_year', 'start_local_dt_month']
    assert(local_freq == LocalFreq.YEARLY)
    return ['start_local_dt_year']

def _get_local_key_to_fill_fn(local_freq):
    """
    If we group by the local time, then the key is a tuple, e.g.
    (year, month, day). We return the beginning of the period to be consistent
    with the date_range grouping
    :param local_freq: the frequency enum
    :return: the function used to fill in the times
    """
    if (local_freq == LocalFreq.DAILY):
        # The key should be (year, month, day)
        return local_dt_fill_times_daily
    elif (local_freq == LocalFreq.MONTHLY):
        # The key should be (year, month)
        return local_dt_fill_times_monthly
    assert(local_freq == LocalFreq.YEARLY)
    # The key should be (year)
    return local_dt_fill_times_yearly

# When we are summarizing by local time, it is challenging to figure out what
# the start timestamp should be. If the whole day was spent in the same timezone
# then we can use that timezone. But what if we switched timezones during the
# course of the day?
#
# For now, we use the start of the day in the timezone of the first section of
# the day.
# Since timestamps do increase monotonically, sections are binned by start time,
# and we know that the user was in the first timezone at the beginning of the day,
# this seems to make sense.
#
# Concretely, India is 5 hours ahead of UTC and the US West Coast is 7-8 hours
# behind UTC.
# So it is possible to fly from India to the US and arrive at the same local time
# i.e. leave on 1st March and arrive on 1st March.
#
# This is how that works wrt timezones
#
# >>> print arrow.Arrow(year=2016, month=3, day=1, tzinfo=tz.gettz("Asia/Calcutta"))
# 2016-03-01T00:00:00+05:30
# >>> print arrow.Arrow(year=2016, month=3, day=1, tzinfo=tz.gettz("Asia/Calcutta")).ceil('day').to('UTC')
# 2016-03-01T18:29:59.999999+00:00
# >>> print arrow.Arrow(year=2016, month=3, day=1, tzinfo=tz.gettz("Asia/Calcutta")).ceil('day').timestamp
# 1456856999
#
# >>> print arrow.Arrow(year=2016, month=3, day=1, tzinfo=tz.gettz("America/Los_Angeles"))
# 2016-03-01T00:00:00-08:00
# >>> print arrow.Arrow(year=2016, month=3, day=1, tzinfo=tz.gettz("America/Los_Angeles")).ceil('day').to('UTC')
# 2016-03-02T07:59:59.999999+00:00
# >>> print arrow.Arrow(year=2016, month=3, day=1, tzinfo=tz.gettz("America/Los_Angeles")).ceil('day').timestamp
# 1456905599
#
# So the end timestamp for that very long day would be 1456905599

def local_dt_fill_times_daily(key, section_group_df, metric_summary):
    first_tz = _get_tz(section_group_df)
    ld = ecwl.LocalDate({'year': key[0],
                         'month': key[1],
                         'day': key[2],
                         'timezone': first_tz})
    dt = arrow.Arrow(ld.year, ld.month, ld.day, tzinfo=first_tz
                     ).floor('day')
    metric_summary.ts = dt.timestamp
    metric_summary.local_dt = ld
    metric_summary.fmt_time = dt.format("YYYY-MM-DD")

def local_dt_fill_times_monthly(key, section_group_df, metric_summary):
    first_tz = _get_tz(section_group_df)
    ld = ecwl.LocalDate({'year': key[0],
                         'month': key[1],
                         'timezone': first_tz})
    dt = arrow.Arrow(ld.year, ld.month, 1,
                     tzinfo=first_tz).floor('month')
    metric_summary.ts = dt.timestamp
    metric_summary.local_dt = ld
    metric_summary.fmt_time = dt.format("YYYY-MM")

def local_dt_fill_times_yearly(key, section_group_df, metric_summary):
    first_tz = _get_tz(section_group_df)
    ld = ecwl.LocalDate({'year': key,
                         'timezone': first_tz})
    dt = arrow.Arrow(ld.year, 1, 1, tzinfo=first_tz
                     ).floor('year')
    metric_summary.ts = dt.timestamp
    metric_summary.local_dt = ld
    metric_summary.fmt_time = dt.format("YYYY")

def _get_tz(section_group_df):
    return section_group_df.sort_values(by='start_ts').head(1).start_local_dt_timezone.iloc[0]
