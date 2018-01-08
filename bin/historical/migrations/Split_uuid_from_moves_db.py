from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from pymongo import MongoClient
from get_database import get_uuid_db, get_moves_db, get_profile_db
from datetime import datetime

for entry in get_moves_db().find():
  print("%s -> %s" % (entry['user'], entry['uuid']))
  userEmail = entry['user']
  userUUID = entry['uuid']
  userUUIDEntry = \
    {
      'user_email': userEmail,
      'uuid': userUUID,
      'update_ts': datetime.now()
    }
  get_uuid_db().update({'user_email': userEmail}, userUUIDEntry, upsert=True)

  profileUpdateObj = {'user_id': userUUID, 'study_list': [], 'update_ts': datetime.now()}
  get_profile_db().update({'user_id': userUUID}, {'$set': profileUpdateObj}, upsert=True)

  get_moves_db().update({'uuid': userUUID}, {'$set': {'our_uuid': userUUID}})
  get_moves_db().update({'uuid': userUUID}, {'$unset': {'uuid': "", 'user': ""}})
  
