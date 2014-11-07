import unittest
import json
from pymongo import MongoClient
# Needed to modify the pythonpath
import sys
import os

# print "old path is %s" % sys.path
sys.path.append("%s/../CFC_WebApp/" % os.getcwd())
sys.path.append("%s" % os.getcwd())
# print "new path is %s" % sys.path

from utils import load_database_json, purge_database_json
from main import auth
from moves import collect

sampleAuthMessage = {'user_id': 99999999999999999, 'access_token': 'Ignore_me', 'expires_in': 15551999, 'token_type': 'bearer', 'refresh_token': 'Ignore_me'}

def loadMovesInputFile(userEmail, fileName):
  # load_database_json.loadTable("localhost", "Test_Groups", "tests/data/groups.json")
  # load_database_json.loadTable("localhost", "Test_Modes", "tests/data/modes.json")
  from dao.user import User

  user = User.fromEmail(userEmail)
  savedTokens = auth.getAccessToken(user.uuid)
  print savedTokens
  if len(savedTokens) == 0:
    auth.saveAccessToken(sampleAuthMessage, user.uuid)
  result = json.load(open(fileName))
  print json.dumps(result)
  collect.processResult(user.uuid, result)  

if __name__ == '__main__':
    loadMovesInputFile(sys.argv[1], sys.argv[2])
