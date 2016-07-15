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



def create_habit(user_id, new_habit):
  method_uri = "/api/v3/tasks/user"
  get_habits_uri = method_uri + "?type=habits"
  #First, get all habits and check if the habit requested already exists
  result = proxy.habiticaProxy(user_id, 'GET', get_habits_uri, None)
  habits = result.json()
  for habit in habits['data']:
    if habit['text'] == new_habit['text']:
      #if the habit requested already exists, return it
      return habit['_id']
  #if habit not found, create habit
  response = proxy.habiticaProxy(user_id, 'POST', method_uri, new_habit)
  habit_created = response.json()
  return habit_created['data']['_id']



def reward_active_transportation(self, user_id):
  #make sure habits exist
  #bike
  bike_habit = {'type': "habit", 'text': "Bike", 'up': True, 'down': False, 'priority': 2}
  bike_habit_id = create_habit(user_id, bike_habit)
  #walk
  walk_habit = {'type': "habit", 'text': "Walk", 'up': True, 'down': False, 'priority': 2}
  walk_habit_id = create_habit(user_id, walk_habit)

  #get timestamps
  user_val = list(edb.get_habitica_db().find({"user_id": user_id}))[0]
  timestamp_from_db = user_val['last_timestamp']
  timestamp_now = arrow.utcnow().timestamp
  
  #FIX! how do I change the frequency to be just one segment? For now I just left it as yearly
  summary_ts = earmt.group_by_timestamp(user, timestamp_from_db, timestamp_now, earmt.LocalFreq.YEARLY, earmts.get_distance)

  #FIX! is this getting the distances properly? Is it possible to have a function that calls "create sample data" to test the metrics
  bike_distance = summary_ts[0].BICYCLING
  walk_distance = summary_ts[0].ON_FOOT

  method_uri_walk = "/api/v3/tasks/"+ walk_habit_id + "/score/up"
  method_uri_bike = "/api/v3/tasks/"+ bike_habit_id + "/score/up"
  #FIX! consider saving the mod (eg bike_distance%3000) to count towards the next reading
  #FIX! consider creating separate function to "score_habit"
  #reward user by + habits
  # Walk: 1 plus for every km
  for i in range(walk_distance//1000):
    res = proxy.habiticaProxy(user_id, 'POST', method_uri_walk, None)
  # Bike: 1 plus for every 3 km
  for i in range(bike_distance//3000):
    res2 = proxy.habiticaProxy(user_id, 'POST', method_uri_bike, None)



def auto_complete_tasks(user_id):
  reward_active_transportation(user_id)

