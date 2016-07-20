# Standard imports
import json
import requests
import logging
import arrow
import urllib2

# Our imports
import emission.core.get_database as edb
import emission.net.ext_service.habitica.setup as assist

key_file = open('conf/net/keys.json')
key_data = json.load(key_file)
url = key_data["habitica"]["url"]



def habiticaRegister(username, email, password, our_uuid):
  user_dict = {}
  #try to login and see if user is already in Habitica
  try:
    login_url = url + '/api/v3/user/auth/local/login'
    user_request = {'username': username,'email': email,'password': password}
    logging.debug("About to login %s"% user_request)
    u = requests.post(login_url, json=user_request)
    user_dict = json.loads(u.text)
    logging.debug("parsed json from habitica has keys = %s" % user_dict.keys())

  #if u fails, then user is not in Habitica, so needs to create an account
  except urllib2.HTTPError:
    register_url = url + '/api/v3/user/auth/local/register'
    user_request = {'username': username,'email': email,'password': password,'confirmPassword': password}
    logging.debug("About to register %s"% user_request)
    u = requests.post(register_url, json=user_request)
    # Bail out if we get an error
    u.raise_for_status()
    user_dict = json.loads(u.text)
    logging.debug("parsed json from habitica has keys = %s" % user_dict.keys())

  #if user is not in e-mission db, create it
  if edb.get_habitica_db().find({'user_id': our_uuid}).count() == 0:
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
      'habitica_id': user_dict['data']['_id'], 
      'habitica_token': user_dict['data']['apiToken'],
      'habitica_group_id': ""})
  
  #Do initial setup and append GroupId to the user
  group_id = assist.user_setup(our_uuid)
  user_dict['group_id'] = group_id
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
