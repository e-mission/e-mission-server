# Standard imports
import json
import logging
import uuid
import random

# Our imports
import emission.core.get_database as edb
import emission.net.ext_service.habitica.proxy as proxy
import emission.net.ext_service.habitica.sync_habitica as hab


def setup_default_habits(user_id):
  bike_habit = {'type': "habit", 'text': "Bike", 'up': True, 'down': False, 'priority': 2}
  bike_habit_id = hab.create_habit(user_id, bike_habit)
  walk_habit = {'type': "habit", 'text': "Walk", 'up': True, 'down': False, 'priority': 2}
  walk_habit_id = hab.create_habit(user_id, walk_habit)


def setup_party(user_id):
  #check if user is already in a party

  #parties = [{"leader": "Juliana", "group_id": "488cae51-aeee-4004-9fa0-dd4219a3a77e"}, {"leader": "Sunil", "group_id": "751e5f9a-bd2d-4c4c-ba81-6fb89bccdf5d"}, {"leader": "Shankari", "group_id": "93c35a70-f70e-4d6e-ac2b-3e1c81fedf0f"}]
  parties = ["Juliana", "Sunil", "Shankari"]
  group = random.randint(0, 2)
  leader_val = list(edb.get_habitica_db().find({"habitica_username": parties[group]}))[0]
  leader_id = leader_val['user_id']
  leader = uuid.UUID(leader_id)
  groupId = leader_val['habitica_group_id']
  invite_uri = "/api/v3/groups/"+groupId+"/invite"
  
  user_val = list(edb.get_habitica_db().find({"user_id": user_id}))[0]
  method_args = {'uuids': [user_val['habitica_id']], 'inviter': parties[group], 'emails': []}
  response = proxy.habiticaProxy(user_uuid, 'POST', invite_uri, method_args)
  edb.get_habitica_db().update({"user_id": user_id},{"$set": {'habitica_group_id': groupId}})
  return groupId



def user_setup(user_id):
  setup_default_habits(user_id)
  return setup_party(user_id)

