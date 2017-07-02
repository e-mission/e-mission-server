import logging

import emission.analysis.result.metrics.time_grouping as earmt
import emission.analysis.result.metrics.simple_metrics as earms

def summarize_by_timestamp(user_id, start_ts, end_ts, freq, metric_list, include_aggregate):
    return _call_group_fn(earmt.group_by_timestamp, user_id, start_ts, end_ts,
                          freq, metric_list, include_aggregate)

def summarize_by_local_date(user_id, start_ld, end_ld, freq_name, metric_list, include_aggregate):
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





