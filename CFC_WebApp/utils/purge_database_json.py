from pymongo import MongoClient
import json
import sys
from get_database import get_db, get_section_db

def purgeData(serverName, userName):
  Sections=get_section_db()
  Sections.remove({'user_id' : userName})

def purgeAllData(serverName):
  from tests import common
  db = get_db()
  common.dropAllCollections(db)

if __name__ == '__main__':
  if len(sys.argv) == 0:
    print "USAGE: %s <serverName> [userName]" % sys.argv[0]
    exit(1)

  if len(sys.argv) == 2:
    purgeAllData(sys.argv[1])
  else:
    purgeData(sys.argv[1], sys.argv[2])
