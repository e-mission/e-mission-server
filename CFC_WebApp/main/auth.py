import logging
import json
from moves import Moves
from pymongo import MongoClient
from get_database import get_moves_db

key_file = open('keys.json')
key_data = json.load(key_file)
client_id = key_data["moves"]["client_id"]
client_secret = key_data["moves"]["client_secret"]
https_redirect_url = key_data["moves"]["https_redirect_url"]
ios_redirect_url = key_data["moves"]["ios_redirect_url"]

def movesCallback(code, state, our_uuid):
  logging.debug('code: '+ code + ' state: '+ state + 'user: '+str(our_uuid))
  logging.debug('state = %s with length %d' % (state, len(state)))
  if (len(state) == 0):
    # This is from iOS, we use the custom redirect URL
    redirectUrl = ios_redirect_url
  else:
    # This is from android, so we use the HTTPS one
    redirectUrl = https_redirect_url

  m = Moves(client_id = client_id,
            client_secret = client_secret,
            redirect_url = redirectUrl)

  token = m.auth(request_token = code)
  logging.debug("token = %s" % token)

  moves_access_info = m.access_json
  logging.debug("Response from moves = %s" % moves_access_info)
  # We got a good response from moves.
  # Now we generate a uuid
  saveAccessToken(moves_access_info, our_uuid)

def saveAccessToken(moves_access_info, our_uuid):
  moves_access_info["our_uuid"] = our_uuid
  moves_uid = moves_access_info["user_id"]
  moves_access_info["_id"] = moves_uid

  user_access_table = get_moves_db()
  if (user_access_table.find({"_id": moves_uid}).count() == 0):
    user_access_table.insert(moves_access_info)
  else:
    user_access_table.update({"_id": moves_uid}, moves_access_info)

def getAccessToken(user):
  user_access_table = get_moves_db()
  user_records = []
  for user in user_access_table.find({'our_uuid' : user}):
    user_records.append(user)
  return user_records

def deleteAllTokens(user):
  user_access_table = get_moves_db()
  user_access_table.remove({"our_uuid" : user})

