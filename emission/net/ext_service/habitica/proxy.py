from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import json
import requests
import logging
import urllib.request, urllib.error, urllib.parse
import uuid
import random


# Our imports
import emission.core.get_database as edb


try:
    key_file = open('conf/net/ext_service/habitica.json')
    key_data = json.load(key_file)
    key_file.close()
    url = key_data["url"]
except:
    logging.exception("habitica not configured, game functions not supported")


def habiticaRegister(username, email, password, our_uuid):
  user_dict = {}
  #if user is already in e-mission db, try to load user data
  if edb.get_habitica_db().count_documents({'user_id': our_uuid}) == 1:
    try:
      result = habiticaProxy(our_uuid, 'GET', '/api/v3/user', None)
      user_dict = result.json()
      logging.debug("parsed json from GET habitica user = %s" % user_dict)

    #if it fails, then user is in db but not in Habitica, so needs to create new account
    #FIX! Still need to test if this will throw an error correctly
    except urllib.error.HTTPError:
      user_dict = newHabiticaUser(username, email, password, our_uuid)
      edb.get_habitica_db().update({"user_id": our_uuid},{"$set":
        initUserDoc(our_uuid, username, password, user_dict)
      },upsert=True)
      #if user_dict['data']['party']['_id']:
        #edb.get_habitica_db().update({"user_id": our_uuid},{"$set": {'habitica_group_id': user_dict['data']['party']['_id']}},upsert=True)

    #now we have the user data in user_dict, so check if db is correct
    #Fix! should prob check here if our db is right

  #if user is not in db, try to log in using email and password
  else:
    try:
      login_url = url + '/api/v3/user/auth/local/login'
      user_request = {'username': username,'email': email,'password': password}
      logging.debug("About to login %s"% user_request)
      login_response = requests.post(login_url, json=user_request)
      logging.debug("response = %s" % login_response)

      #if 401 error, then user is not in Habitica, so create new account and pass user to user_dict
      if login_response.status_code == 401:
        user_dict = newHabiticaUser(username, email, password, our_uuid)
      else:
        logging.debug("habitica http response from login = %s" % login_response)
        user_auth = json.loads(login_response.text)
        logging.debug("parsed json from habitica has keys = %s" % user_auth)
        #login only returns user auth headers, so now get authenticated user and put it in user_dict
        auth_headers = {'x-api-user': user_auth['data']['id'], 'x-api-key': user_auth['data']['apiToken']}
        get_user_url = url + '/api/v3/user'
        result = requests.request('GET', get_user_url, headers=auth_headers, json={})
        logging.debug("result = %s" % result)
        result.raise_for_status()
        user_dict = result.json()
        user_dict['data']['apiToken'] = user_auth['data']['apiToken']
        logging.debug("parsed json from GET habitica user = %s" % user_dict)

    #If if fails to login AND to create new user, throw exception
    except:
      logging.exception("Exception while trying to login/signup!")
    
    logging.debug("habitica user to be created in our db = %s" % user_dict['data'])  
    #Now save new user (user_dict) to our db
    #Since we are randomly generating the password, we store it in case users 
    #want to access their Habitica account from the browser
    #Need to create a way from them to retrieve username/password
    #metrics_data is used to calculate points based on km biked/walked
    #last_timestamp is the last time the user got points, and bike/walk_count are the leftover km
    habitica_user_table = edb.get_habitica_db()
    insert_doc = initUserDoc(our_uuid, username, password, user_dict)
    insert_doc.update({'user_id': our_uuid})
    habitica_user_table.insert(insert_doc)

    #Since we have a new user in our db, create its default habits (walk, bike)
    setup_default_habits(our_uuid)
  return user_dict

def initUserDoc(user_id, username, password, user_dict):
  return {'task_state': {},
       'habitica_username': username,
       'habitica_password': password,
       'habitica_id': user_dict['data']['_id'],
       'habitica_token': user_dict['data']['apiToken']}


def newHabiticaUser(username, email, password, our_uuid):
  register_url = url + '/api/v3/user/auth/local/register'
  user_request = {'username': username,'email': email,'password': password,'confirmPassword': password}
  logging.debug("About to register %s"% user_request)
  u = requests.post(register_url, json=user_request)
  # Bail out if we get an error
  u.raise_for_status()
  user_dict = json.loads(u.text)
  logging.debug("parsed json from habitica has keys = %s" % list(user_dict.keys()))
  return user_dict


def habiticaProxy(user_uuid, method, method_url, method_args):
  logging.debug("For user %s, about to proxy %s method %s with args %s" %
                (user_uuid, method, method_url, method_args))
  stored_cfg = get_user_entry(user_uuid)
  auth_headers = {'x-api-user': stored_cfg['habitica_id'],
                  'x-api-key': stored_cfg['habitica_token']}
  logging.debug("auth_headers = %s" % auth_headers)
  habitica_url = url + method_url
  result = requests.request(method, habitica_url,
                            headers=auth_headers,
                            json=method_args)
  logging.debug("result = %s" % result)
  result.raise_for_status()
  # result['testing'] = 'test'
  temp = result.json()
  temp['auth'] = {'apiId': stored_cfg['habitica_id'],
                  'apiToken': stored_cfg['habitica_token']}
  result.encoding, result._content = 'utf8', json.dumps(temp).encode()
  return result


def setup_party(user_id, group_id_from_url, inviterId):
  #check if user is already in a party
  method_url = "/api/v3/user"
  result = habiticaProxy(user_id, 'GET', method_url, None)
  data = result.json()
  if '_id' in data['data']['party']:
    group_id = data['data']['party']['_id']
    logging.info("User %s is already part of group %s" % (user_id, group_id))
    raise RuntimeError("User %s is already a part of group %s" % (user_id, group_id))
  #if the user is not already in a party, then add them to the party to which they were invited
  else:
    group_id = group_id_from_url
    invite_uri = "/api/v3/groups/"+group_id+"/invite"
    logging.debug("invite user to party api url = %s" % invite_uri)
    user_val = list(edb.get_habitica_db().find({"user_id": user_id}))[0]
    method_args = {'uuids': [user_val['habitica_id']], 'inviter': group_id, 'emails': []}
    emInviterId = edb.get_habitica_db().find_one({"habitica_id": inviterId})["user_id"]
    response = habiticaProxy(emInviterId, 'POST', invite_uri, method_args)
    logging.debug("invite user to party response = %s" % response)
    join_url = "/api/v3/groups/"+group_id+"/join"
    response2 = habiticaProxy(user_id, 'POST', join_url, {})
    response.raise_for_status()
    response2.raise_for_status()
  return group_id


def setup_default_habits(user_id):
  bike_walk_habit = {'type': "habit", 'text': "Bike and Walk", 'notes': "Automatically get points for every 1 km walked or biked. ***=== DO NOT EDIT BELOW THIS POINT ===*** AUTOCHECK: {\"mapper\": \"active_distance\", \"args\": {\"walk_scale\": 1000, \"bike_scale\": 1000}}", 'up': True, 'down': False, 'priority': 2}
  bike_walk_habit_id = create_habit(user_id, bike_walk_habit)
  invite_friends = {'type': "habit", 'text': "Spread the word", 'notes': "Get points for inviting your friends! We're better together.", 'up': True, 'down': False, 'priority': 2}
  invite_friends_id = create_habit(user_id, invite_friends)

def create_habit(user_id, new_habit):
  method_uri = "/api/v3/tasks/user"
  get_habits_uri = method_uri + "?type=habits"
  #First, get all habits and check if the habit requested already exists
  result = habiticaProxy(user_id, 'GET', get_habits_uri, None)
  habits = result.json()
  for habit in habits['data']:
    if habit['text'] == new_habit['text']:
      #if the habit requested already exists, return it
      return habit['_id']
  #if habit not found, create habit
  response = habiticaProxy(user_id, 'POST', method_uri, new_habit)
  habit_created = response.json()
  return habit_created['data']['_id']

# Should we have an accessor class for this?
# Part of the integration, not part of the standard timeseries

def get_user_entry(user_id):
  user_query = {'user_id': user_id}
  # TODO: Raise a real, descriptive exception here instead of asserting
  assert(edb.get_habitica_db().count_documents(user_query) == 1)
  stored_cfg = edb.get_habitica_db().find_one(user_query)
  return stored_cfg

def save_user_entry(user_id, user_entry):
  assert(user_entry["user_id"] == user_id)
  return edb.save(edb.get_habitica_db(), user_entry)
