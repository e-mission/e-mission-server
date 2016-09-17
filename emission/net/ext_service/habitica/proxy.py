# Standard imports
import json
import requests
import logging
import arrow
import urllib2
import uuid
import random


# Our imports
import emission.core.get_database as edb



key_file = open('conf/net/keys.json')
key_data = json.load(key_file)
url = key_data["habitica"]["url"]



def habiticaRegister(username, email, password, our_uuid):
  user_dict = {}
  #if user is already in e-mission db, try to load user data
  if edb.get_habitica_db().find({'user_id': our_uuid}).count() == 1:
    try:
      result = habiticaProxy(our_uuid, 'GET', '/api/v3/user', None)
      user_dict = result.json()
      logging.debug("parsed json from GET habitica user = %s" % user_dict)

    #if it fails, then user is in db but not in Habitica, so needs to create new account
    #FIX! Still need to test if this will throw an error correctly
    except urllib2.HTTPError:
      user_dict = newHabiticaUser(username, email, password, our_uuid)
      edb.get_habitica_db().update({"user_id": our_uuid},{"$set": {'metrics_data': {'last_timestamp': arrow.utcnow().timestamp, 'bike_count': 0, 'walk_count': 0},
      'habitica_username': username, 
      'habitica_password': password, 
      'habitica_id': user_dict['data']['_id'], 
      'habitica_token': user_dict['data']['apiToken'],
      'habitica_group_id': None}},upsert=True)
      if user_dict['data']['party']['_id']:
        edb.get_habitica_db().update({"user_id": our_uuid},{"$set": {'habitica_group_id': user_dict['data']['party']['_id']}},upsert=True)


    #now we have the user data in user_dict, so check if db is correct
    #Fix! should prob check here if our db is right, if it's in group, etc

  #if user is not in db, try to log in using email and password
  else:
    try:
      login_url = url + '/api/v3/user/auth/local/login'
      user_request = {'username': username,'email': email,'password': password}
      logging.debug("About to login %s"% user_request)
      login_response = requests.post(login_url, json=user_request)
      logging.debug("response = %s" % login_response)
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

    #if it fails, then user is also not in Habitica, so needs to create new account and put it in user_dict
    #FIX!! throw except only if u returns a 401 error
    except:
      logging.exception("Exception while trying to login!")
    
    logging.debug("habitica user to be created in our db = %s" % user_dict['data'])  
    #Now save new user (user_dict) to our db
    #Since we are randomly generating the password, we store it in case users 
    #want to access their Habitica account from the browser
    #Need to create a way from them to retrieve username/password
    #metrics_data is used to calculate points based on km biked/walked
    #last_timestamp is the last time the user got points, and bike/walk_count are the leftover km
    habitica_user_table = edb.get_habitica_db()
    habitica_user_table.insert({'user_id': our_uuid, 
      'metrics_data': {'last_timestamp': arrow.utcnow().timestamp, 'bike_count': 0, 'walk_count': 0},
      'habitica_username': username, 
      'habitica_password': password, 
      'habitica_id': user_dict['data']['id'], 
      'habitica_token': user_dict['data']['apiToken'],
      'habitica_group_id': None})

    #Since we have a new user in our db, create its default habits (walk, bike)
    setup_default_habits(our_uuid)
  return user_dict


def newHabiticaUser(username, email, password, our_uuid):
  register_url = url + '/api/v3/user/auth/local/register'
  user_request = {'username': username,'email': email,'password': password,'confirmPassword': password}
  logging.debug("About to register %s"% user_request)
  u = requests.post(register_url, json=user_request)
  # Bail out if we get an error
  u.raise_for_status()
  user_dict = json.loads(u.text)
  logging.debug("parsed json from habitica has keys = %s" % user_dict.keys())
  return user_dict


def habiticaProxy(user_uuid, method, method_url, method_args):
  logging.debug("For user %s, about to proxy %s method %s with args %s" %
                (user_uuid, method, method_url, method_args))
  user_query = {'user_id': user_uuid}
  assert(edb.get_habitica_db().find(user_query).count() == 1)
  stored_cfg = edb.get_habitica_db().find_one(user_query)
  auth_headers = {'x-api-user': stored_cfg['habitica_id'],
                  'x-api-key': stored_cfg['habitica_token']}
  logging.debug("auth_headers = %s" % auth_headers)
  habitica_url = url + method_url
  result = requests.request(method, habitica_url,
                            headers=auth_headers,
                            json=method_args)
  logging.debug("result = %s" % result)
  result.raise_for_status()
  return result


def setup_party(user_id, group_id_from_url, inviterId):
  group_id = list(edb.get_habitica_db().find({"user_id": user_id}))[0]['habitica_group_id']
  if group_id is None:
    #check if user is already in a party
    try:
      method_url = "/api/v3/user"
      result = habiticaProxy(user_id, 'GET', method_url, None)
      data = result.json()
      group_id = data['data']['party']['_id']
      edb.get_habitica_db().update({"user_id": user_id},{"$set": {'habitica_group_id': group_id}},upsert=True)

    except KeyError:
      group_id = group_id_from_url
      invite_uri = "/api/v3/groups/"+group_id+"/invite"
      logging.debug("invite user to party api url = %s" % invite_uri)
      user_val = list(edb.get_habitica_db().find({"user_id": user_id}))[0]
      method_args = {'uuids': [inviterId], 'inviter': group_id, 'emails': []}
      response = habiticaProxy(inviterId, 'POST', invite_uri, method_args)
      logging.debug("invite user to party response = %s" % response)
      join_url = "/api/v3/groups/"+group_id+"/join"
      response2 = habiticaProxy(user_id, 'POST', join_url, {})
      response.raise_for_status()
      response2.raise_for_status()
      edb.get_habitica_db().update({"user_id": user_id},{"$set": {'habitica_group_id': group_id}},upsert=True)
  return group_id

def setup_default_habits(user_id):
  bike_habit = {'type': "habit", 'text': "Bike", 'notes': "3 km = 1+ (calculated automatically)", 'up': True, 'down': False, 'priority': 2}
  bike_habit_id = create_habit(user_id, bike_habit)
  walk_habit = {'type': "habit", 'text': "Walk", 'notes': "1 km = 1+ (calculated automatically)", 'up': True, 'down': False, 'priority': 2}
  walk_habit_id = create_habit(user_id, walk_habit)

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
