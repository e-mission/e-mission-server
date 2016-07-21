# Standard imports
import json
import requests
import logging
import arrow
import attrdict as ad

# Our imports
import emission.core.get_database as edb
import emission.net.ext_service.habitica.proxy as proxy
import emission.analysis.result.metrics.simple_metrics as earmts
import emission.analysis.result.metrics.time_grouping as earmt



def reward_active_transportation(user_id):
  #make sure habits exist
  #bike
  bike_habit = {'type': "habit", 'text': "Bike", 'up': True, 'down': False, 'priority': 2}
  bike_habit_id = proxy.create_habit(user_id, bike_habit)
  #walk
  walk_habit = {'type': "habit", 'text': "Walk", 'up': True, 'down': False, 'priority': 2}
  walk_habit_id = proxy.create_habit(user_id, walk_habit)

  #get timestamps
  user_val = list(edb.get_habitica_db().find({"user_id": user_id}))[0]['metrics_data']
  timestamp_from_db = user_val['last_timestamp']
  timestamp_now = arrow.utcnow().timestamp
  
  #Get metrics
  summary_ts = earmt.group_by_timestamp(user_id, timestamp_from_db, timestamp_now, None, earmts.get_distance)

  #get distances leftover from last timestamp
  bike_distance = user_val['bike_count']
  walk_distance = user_val['walk_count']

  #iterate over summary_ts and look for bike/on foot
  for item in summary_ts:
    try:
        bike_distance += item.BICYCLING
    except AttributeError:
        pass
    try:
        walk_distance = item.ON_FOOT
    except AttributeError:
        pass
  
  method_uri_walk = "/api/v3/tasks/"+ walk_habit_id + "/score/up"
  method_uri_bike = "/api/v3/tasks/"+ bike_habit_id + "/score/up"
  #reward user by scoring + habits
  # Walk: +1 for every km
  walk_pts = int(walk_distance//1000)
  for i in range(walk_pts):
    res = proxy.habiticaProxy(user_id, 'POST', method_uri_walk, None)
  # Bike: +1 for every 3 km
  bike_pts = int(bike_distance//3000)
  for i in range(bike_pts):
    res2 = proxy.habiticaProxy(user_id, 'POST', method_uri_bike, None)

  #update the timestamp and bike/walk counts in db
  edb.get_habitica_db().update({"user_id": user_id},{"$set": {'metrics_data': {'last_timestamp': arrow.utcnow().timestamp, 'bike_count': bike_distance%3000, 'walk_count': walk_distance%1000}}})



def auto_complete_tasks(user_id):
  reward_active_transportation(user_id)

