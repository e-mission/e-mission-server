# Standard imports
import json
import requests
import logging
import arrow
import attrdict as ad
import copy

# Our imports
import emission.core.get_database as edb
import emission.net.ext_service.habitica.proxy as proxy
import emission.analysis.result.metrics.simple_metrics as earmts
import emission.analysis.result.metrics.time_grouping as earmt
import emission.storage.pipeline_queries as esp

# AUTOCHECK: {"mapper": "active_distance", "args": {"walk_scale": 1000,
#                                                   "bike_scale": 3000}}

def give_points(user_id, task, curr_state):

    #get timestamps
    # user_val = list(edb.get_habitica_db().find({"user_id": user_id}))[0]['metrics_data']

    # Note that
    walk_scale = task["args"]["walk_scale"]
    bike_scale = task["args"]["bike_scale"]

    if curr_state is None:
        timestamp_from_db = arrow.utcnow().timestamp
        leftover_bike = 0
        leftover_walk = 0
        curr_state = {'last_timestamp': timestamp_from_db,
                     'bike_count': leftover_bike,
                     'walk_count': leftover_walk}
    else:
        timestamp_from_db = curr_state['last_timestamp']
        leftover_bike = curr_state["bike_count"]
        leftover_walk = curr_state["walk_count"]

    timestamp_now = arrow.utcnow().timestamp
    
    #Get metrics
    summary_ts = earmt.group_by_timestamp(user_id, timestamp_from_db, timestamp_now,
                                          None, [earmts.get_distance])
    logging.debug("Metrics response: %s" % summary_ts)
    
    if summary_ts["last_ts_processed"] == None:
      new_state = curr_state
    else:
      #get distances leftover from last timestamp
      bike_distance = leftover_bike
      walk_distance = leftover_walk

      #iterate over summary_ts and look for bike/on foot
      for item in summary_ts["result"][0]:
        try:
            bike_distance += item.BICYCLING
            logging.debug("bike_distance += %s" % item.BICYCLING)
        except AttributeError:
            logging.debug("no bike")
        try:
            walk_distance += item.ON_FOOT
            logging.debug("walk_distance += %s" % item.ON_FOOT)
        except AttributeError:
            logging.debug("no Android walk")
        try:
            walk_distance += item.WALKING
            logging.debug("walk_distance += %s" % item.WALKING)
        except AttributeError:
            logging.debug("no ios walk")
        try:
            walk_distance += item.RUNNING
            logging.debug("walk_distance += %s" % item.RUNNING)
        except AttributeError:
            logging.debug("no running")
      
      logging.debug("Finished with bike_distance == %s" % bike_distance)
      logging.debug("Finished with walk_distance == %s" % walk_distance)

      method_uri_active_distance = "/api/v3/tasks/"+ task.task_id + "/score/up"
      #reward user by scoring + habits
      # Walk: +1 for every km
      walk_pts = int(walk_distance//walk_scale)
      for i in range(walk_pts):
        res = proxy.habiticaProxy(user_id, 'POST', method_uri_active_distance, None)
        logging.debug("Request to score walk points %s" % res)
      # Bike: +1 for every 3 km
      bike_pts = int(bike_distance//bike_scale)
      for i in range(bike_pts):
        res2 = proxy.habiticaProxy(user_id, 'POST', method_uri_active_distance, None)
        logging.debug("Request to score bike points %s" % res2)

      #update the timestamp and bike/walk counts in db
      new_state = {'last_timestamp': summary_ts["last_ts_processed"] + esp.END_FUZZ_AVOID_LTE,
              'bike_count': bike_distance%bike_scale,
              'walk_count': walk_distance%walk_scale}

    logging.debug("Returning %s" % new_state)
    return new_state

def reset_to_ts(user_id, ts, task, curr_state):
    new_state = copy.copy(curr_state)
    new_state['last_timestamp'] = ts
    # We don't know what the leftover walk/bike stuff without re-running from
    # scratch, so let's leave it untouched. Error can be max 1 point, which is
    # not too bad.
    return new_state

