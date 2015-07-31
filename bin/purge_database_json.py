from pymongo import MongoClient
import json
import sys
from emission.core.get_database import get_db, get_section_db
from emission.tests import common

def purgeData(userName):
  Sections=get_section_db()
  common.purgeData(Sections)

def purgeAllData():
  db = get_db()
  common.dropAllCollections(db)

if __name__ == '__main__':
  if len(sys.argv) == 0:
    print "USAGE: %s [userName]" % sys.argv[0]
    exit(1)

  if len(sys.argv) == 1:
    purgeAllData()
  else:
    purgeData(sys.argv[1])
