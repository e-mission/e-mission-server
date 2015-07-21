__author__ = 'Yin'
from pymongo import MongoClient
import uuid
from get_database import get_mode_db, get_section_db, get_trip_db, get_user_db

Users=get_user_db()
for user in Users.find():
    userEmail=user['user']
    if 'uuid' not in user:
    	print "Generating UUID for user %s" % userEmail
    	user['uuid']=uuid.uuid3(uuid.NAMESPACE_URL, "mailto:%s" % userEmail.encode("UTF-8"))
    	print "Generated UUID is %s" % user['uuid']
	get_user_db().update(
			{'user': userEmail},
			{
			   "$set" : {"uuid": user['uuid']}
			})
    else:
	print "Using existing UUID %s for user %s" % (user['uuid'], userEmail)
