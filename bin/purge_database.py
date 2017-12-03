from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from pymongo import MongoClient
import json
import sys
from emission.core.get_database import get_db, get_section_db
import emission.tests.common as etc

def purgeAllData():
  db = get_db()
  etc.dropAllCollections(db)

if __name__ == '__main__':
  if len(sys.argv) != 1:
    print("USAGE: %s" % sys.argv[0])
    exit(1)

  purgeAllData()
