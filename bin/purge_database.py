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
import emission.core.get_database as edb
import emission.tests.common as etc

def purgeAllData():
  db = edb._get_current_db()
  etc.dropAllCollections(db)

if __name__ == '__main__':
  if len(sys.argv) != 1:
    print("USAGE: %s" % sys.argv[0])
    exit(1)

  purgeAllData()
