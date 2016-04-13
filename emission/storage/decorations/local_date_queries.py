import logging
import pymongo
import arrow

import emission.core.wrapper.localdate as ecwl

def get_local_date(ts, timezone):
    adt = arrow.get(ts).to(timezone)
    return ecwl.LocalDate({'year': adt.year, 'month': adt.month, 'day': adt.day,
    'hour': adt.hour, 'minute': adt.minute, 'second': adt.second,
    'weekday': adt.isoweekday(), 'timezone': timezone})

def get_range_query(field_name, start_local_dt, end_local_dt):
    if start_local_dt.keys() != end_local_dt.keys():
        raise RuntimeError("start_local_dt.keys() = %s does not match end_local_dt.keys() = %s" %
            (start_local_dt.keys(), end_local_dt.keys()))
    query_result = {}
    for key in start_local_dt:
        curr_field = "%s.%s" % (field_name, key)
        query_result.update({curr_field: {"$gte": start_local_dt[key],
                                           "$lte": end_local_dt[key]}})
    logging.debug("Returning query %s" % query_result)
    return query_result
