# Standard imports
import json
import requests
import logging

# Our imports
from emission.core.get_database import get_habitica_db

key_file = open('conf/net/keys.json')
key_data = json.load(key_file)
url = key_data["habitica"]["url"]

def habiticaRegister(username, email, password, our_uuid):

  register_url = url + '/api/v3/user/auth/local/register'
  user_request = {'username': username,'email': email,'password': password,'confirmPassword': password}
  u = requests.post(register_url, json=user_request)
  user_dict = json.loads(u.text)
  logging.debug("parsed json from habitica = %s" % user_dict)

  #Since we are randomly generating the password, we store it in case users 
  #want to access their Habitica account from the browser
  #Need to create a way from them to retrieve username/password
  habitica_user_table = get_habitica_db()
  habitica_user_table.insert({'user_id': our_uuid, 'habitica_username': username, 
    'habitica_password': password, 'habitica_id': user_dict['data']['_id'], 
    'habitica_token': user_dict['data']['apiToken']})
