from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import zip
from builtins import *
import logging
import asyncio

import emission.core.get_database as edb
import emission.analysis.result.metrics.time_grouping as earmt
import emission.analysis.result.metrics.simple_metrics as earms
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.decorations.local_date_queries as esdl
import emission.storage.timeseries.fmt_time_query as estf

import emcommon.metrics.metrics_summaries as emcmms


def get_agg_metrics_from_db(start_ymd, end_ymd):
    logging.debug("get_agg_metrics(%s, %s)" % (start_ymd, end_ymd))
    query = estf.FmtTimeQuery("date", start_ymd, end_ymd).get_query()
    metric_cursor = edb.get_agg_metrics_db().find(query)
    metric_docs = list(metric_cursor)
    logging.debug("AggMetrics DB had %d entries for %s" %
                  (len(metric_docs), query))
    # Backwards compat to get summaries into the old format that the phone expects in May 2025
    # After phone changes and waiting a few months, we can remove this
    metric_docs = emcmms.munge_agg_metrics(metric_docs)
    return metric_docs

def summarize_by_timestamp(user_id, start_ts, end_ts, freq, metric_list, include_aggregate, app_config=None):
    return _call_group_fn(earmt.group_by_timestamp, user_id, start_ts, end_ts,
                          freq, metric_list, include_aggregate)

def summarize_by_local_date(user_id, start_ld, end_ld, freq_name, metric_list, include_aggregate, app_config=None):
    local_freq = earmt.LocalFreq[freq_name]
    return _call_group_fn(earmt.group_by_local_date, user_id, start_ld, end_ld,
                          local_freq, metric_list, include_aggregate)

def _call_group_fn(group_fn, user_id, start_time, end_time, freq, metric_list, include_aggregate):
    summary_fn_list = [earms.get_summary_fn(metric_name)
                            for metric_name in metric_list]
    logging.debug(["%s -> %s" % (m, s) for (m, s) in zip(metric_list, summary_fn_list)])
    ret_dict = {}
    if include_aggregate:
        aggregate_metrics = group_fn(None, start_time, end_time,
                                     freq, summary_fn_list)
        ret_dict.update({"aggregate_metrics": aggregate_metrics["result"]})
    if user_id is not None:
        user_metrics = group_fn(user_id, start_time, end_time,
                                     freq, summary_fn_list)
        ret_dict.update({"user_metrics": user_metrics["result"]})
    return ret_dict





