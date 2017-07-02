# Input spec sample at
# emission/net/ext_service/push/sample.specs/point_count.query.sample

# Input: query spec
# Output: list of uuids
# 
import logging

import emission.core.wrapper.motionactivity as ecwm

import emission.storage.decorations.local_date_queries as esdl
import emission.storage.decorations.location_queries as esdlq
import emission.storage.timeseries.geoquery as estg
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.tcquery as esttc
import emission.storage.decorations.analysis_timeseries_queries as esda


def query(spec):
  time_type = spec['time_type']
  if 'from_local_date' in spec and 'to_local_date' in spec:
      start_ld = spec['from_local_date']
      end_ld = spec['to_local_date']
      time_query = esttc.TimeComponentQuery("data.local_dt", start_ld, end_ld)
  elif 'start_time' in spec and 'end_time' in spec:
      start_ts = spec['start_time']
      end_ts = spec['end_time']
      time_query = estt.TimeQuery("data.ts", start_ts, end_ts)
  else:
      time_query = None

  modes = spec['modes']
  region = spec['sel_region']
  logging.debug("Filtering values for modes %s, range %s, region %s" %
        (modes, time_query, region))
  query_fn = uuid_list_query
  uuid_list = query_fn(modes, time_query, region)
  logging.info("matched uuid_list of length = %s = %s" % (len(uuid_list), uuid_list))
  return uuid_list

def uuid_list_query(modes, time_query, region):
    if region is None:
        geo_query = None
    else:
        geo_query = estg.GeoQuery(["data.loc"], region)

    extra_query_list = []
    if modes is not None:
        mode_enum_list = [ecwm.MotionTypes[mode] for mode in modes]
        extra_query_list.append(esdlq.get_mode_query(mode_enum_list))

    loc_entry_df = esda.get_data_df(esda.CLEANED_LOCATION_KEY, user_id=None,
                                      time_query=time_query, geo_query=geo_query,
                                      extra_query_list=extra_query_list)
    if len(loc_entry_df) == 0:
        logging.info("No points found matching query, returning empty list")
        return []

    unique_uuid_list = loc_entry_df.user_id.unique().tolist()
    logging.info("Found %d points with %d unique uuids" % (len(loc_entry_df), len(unique_uuid_list)))
    return unique_uuid_list
