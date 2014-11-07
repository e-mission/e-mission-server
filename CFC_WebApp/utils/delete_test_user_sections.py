# Delete entries for the dummy user
# This needs to be done every time we switch to a new database

from pymongo import MongoClient
from get_database import get_section_db
from uuid import UUID

# Users=get_user_db()
fakeUserUUIDQuery = {'user_id': UUID('{951779de-a10c-3373-b186-c1c9b14b5e38}')}

print "About to delete %s sections" % get_section_db().find(fakeUserUUIDQuery).count()
get_section_db().remove(fakeUserUUIDQuery)
