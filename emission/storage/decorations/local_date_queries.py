import logging
import pymongo
import arrow

import emission.core.wrapper.localdate as ecwl

def get_local_date(ts, timezone):
    adt = arrow.get(ts).to(timezone)
    return ecwl.LocalDate({'year': adt.year, 'month': adt.month, 'day': adt.day,
    'hour': adt.hour, 'minute': adt.minute, 'second': adt.second,
    'weekday': adt.weekday(), 'timezone': timezone})

def get_range_query(field_name, start_local_dt, end_local_dt):
    if start_local_dt.keys() != end_local_dt.keys():
        raise RuntimeError("start_local_dt.keys() = %s does not match end_local_dt.keys() = %s" %
            (start_local_dt.keys(), end_local_dt.keys()))
    query_result = {}
    for key in start_local_dt:
        curr_field = "%s.%s" % (field_name, key)
        gte_lte_query = {}
        try:
            gte_lte_query.update({'$gte': int(start_local_dt[key])})
        except:
            logging.info("start_local_dt[%s] = %s, not an integer, skipping" %
                            (key, start_local_dt[key]))
        try:
            gte_lte_query.update({'$lte': int(end_local_dt[key])})
        except:
            logging.info("end_local_dt[%s] = %s, not an integer, skipping" %
                            (key, end_local_dt[key]))
        if len(gte_lte_query) > 0:
            query_result.update({curr_field: gte_lte_query})
        else:
            logging.info("key %s exists, skipping because upper AND lower range are missing" % key)

    logging.debug("In get_range_query, returning query %s" % query_result)
    return query_result
