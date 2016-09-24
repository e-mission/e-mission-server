import arrow 
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.abstract_timeseries as esta

# return queries for requesting data by checking the public status of requested phones
def get_user_queries(uuids, from_ts, to_ts):
  phone_ts = map(lambda id: esta.TimeSeries.get_time_series(id), uuids)
  phone_public = map(lambda ts: list(ts.find_entries(["eval/public_device"])), phone_ts)

  user_queries = map(lambda id: {'user_id': id}, uuids)

  for i, q in enumerate(user_queries):
    p = phone_public[i]
    if len(p) != 0: 
      # requested phone is pubic, return data for the overlapped time range  
      public_ts1 = p[0]["ts"] 
      ts1 = max(from_ts, public_ts1)
      if len(p) == 2:
        # requested phone has unregisterd as public
        public_ts2 = p[1]["ts"]
        ts2 = min(to_ts, public_ts2)
      if ts1 < ts2:
        time_range = estt.TimeQuery("metadata.write_ts", float(ts1), float(ts2))
        time_query = time_range.get_query()
        q.update(time_query)
      else: 
        # invalid time range 
        q = None
    else: 
      # requested phone is not public
      q = None
  return q 