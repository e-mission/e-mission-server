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

def get_range_query(field_name, start_local_dt, end_local_dt):
    if list(start_local_dt.keys()) != list(end_local_dt.keys()):
        raise RuntimeError("start_local_dt.keys() = %s does not match end_local_dt.keys() = %s" %
            (list(start_local_dt.keys()), list(end_local_dt.keys())))
    query_result = {}
    for key in start_local_dt:
        curr_field = "%s.%s" % (field_name, key)
        gte_lte_query = {}
        try:
            start_int = int(start_local_dt[key])
        except:
            logging.info("start_local_dt[%s] = %s, not an integer, skipping" %
                            (key, start_local_dt[key]))
            continue

        try:
            end_int = int(end_local_dt[key])
        except:
            logging.info("end_local_dt[%s] = %s, not an integer, skipping" %
                            (key, end_local_dt[key]))
            continue

        is_rollover = start_int > end_int

        if is_rollover:
            gte_lte_query = get_rollover_query(start_int, end_int)
        else:
            gte_lte_query = get_standard_query(start_int, end_int)

        if len(gte_lte_query) > 0:
            query_result.update({curr_field: gte_lte_query})
        else:
            logging.info("key %s exists, skipping because upper AND lower range are missing" % key)

    logging.debug("In get_range_query, returning query %s" % query_result)
    return query_result

def get_standard_query(start_int, end_int):
    assert(start_int <= end_int)
    return {'$gte': start_int, '$lte': end_int}

def get_rollover_query(start_int, end_int):
    assert(start_int > end_int)
    return {'$not': {'$gt': end_int, '$lt': start_int}}
