from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import pymongo
import arrow

import emission.core.wrapper.localdate as ecwl

def get_range_query(field_prefix, start_ld, end_ld):
    units = [u for u in ecwl.DATETIME_UNITS if u in start_ld and u in end_ld]
    logging.debug(f'get_range_query: units = {units}')
    try:
        gt_query = get_comparison_query(field_prefix, start_ld, end_ld, units, 'gt')
        lt_query = get_comparison_query(field_prefix, end_ld, start_ld, units, 'lt')
        logging.debug(f'get_range_query: gt_query = {gt_query}, lt_query = {lt_query}')
        return { "$and": [gt_query, lt_query] } if gt_query and lt_query else {}
    except AssertionError as e:
        logging.error(f'Invalid range from {str(start_ld)} to {str(end_ld)}: {str(e)}')
        return None

def get_comparison_query(field_prefix, base_ld, limit_ld, units, gt_or_lt):
    field_name = lambda i: f'{field_prefix}.{units[i]}'
    and_conditions, or_conditions = [], []
    tiebreaker_index = -1
    for i, unit in enumerate(units):
        # the range is inclusive, so if on the last unit we should use $lte / $gte instead of $lt / $gt
        op = f'${gt_or_lt}e' if i == len(units)-1 else f'${gt_or_lt}'
        if tiebreaker_index >= 0:
            tiebreaker_conditions = [{ field_name(j): base_ld[units[j]] } for j in range(tiebreaker_index, i)]
            tiebreaker_conditions.append({ field_name(i): { op: base_ld[unit] }})
            or_conditions.append({ "$and": tiebreaker_conditions })
        elif base_ld[unit] == limit_ld[unit]:
            and_conditions.append({field_name(i): base_ld[unit]})
        else:
            assert (base_ld[unit] < limit_ld[unit]) if gt_or_lt == 'gt' else (base_ld[unit] > limit_ld[unit])
            or_conditions.append({field_name(i): { op: base_ld[unit] }})
            tiebreaker_index = i
    if and_conditions and or_conditions:
        return { "$and": and_conditions + [{ "$or": or_conditions }] }
    elif and_conditions:
        return { "$and": and_conditions }
    elif or_conditions:
        return { "$or": or_conditions }
    else:
        return {}

def yyyy_mm_dd_to_local_date(ymd: str) -> ecwl.LocalDate:
    return ecwl.LocalDate({
        'year': int(ymd[0:4]),
        'month': int(ymd[5:7]),
        'day': int(ymd[8:10])
    })

def get_yyyy_mm_dd_range_query(field_name, start_ymd: str, end_ymd: str) -> dict:
    start_local_date = yyyy_mm_dd_to_local_date(start_ymd)
    end_local_date = yyyy_mm_dd_to_local_date(end_ymd)
    return get_range_query(field_name, start_local_date, end_local_date)
