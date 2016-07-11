# Standard imports
import json
import requests
import logging

# Our imports
import emission.core.get_database as edb

key_file = open('conf/net/keys.json')
key_data = json.load(key_file)
url = key_data["habitica"]["url"]



def habiticaRegister(username, email, password, our_uuid):
  register_url = url + '/api/v3/user/auth/local/register'
  user_request = {'username': username,'email': email,'password': password,'confirmPassword': password}
  logging.debug("About to register %s"% user_request)
  u = requests.post(register_url, json=user_request)
  # Bail out if we get an error
  u.raise_for_status()
  user_dict = json.loads(u.text)
  logging.debug("parsed json from habitica has keys = %s" % user_dict.keys())

  #Since we are randomly generating the password, we store it in case users 
  #want to access their Habitica account from the browser
  #Need to create a way from them to retrieve username/password
  habitica_user_table = edb.get_habitica_db()
  habitica_user_table.insert({'user_id': our_uuid, 'habitica_username': username, 
    'habitica_password': password, 'habitica_id': user_dict['data']['_id'], 
    'habitica_token': user_dict['data']['apiToken']})
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
