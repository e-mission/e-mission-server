# Needed to modify the pythonpath
import sys
import os

# print "old path is %s" % sys.path
sys.path.append("%s/../CFC_WebApp/" % os.getcwd())
sys.path.append("%s" % os.getcwd())
# print "new path is %s" % sys.path

from utils import load_database_json, purge_database_json
from moves import collect

def setupForTesting(serverName):
  load_database_json.loadTable(serverName, "Test_Groups", "tests/data/groups.json")
  load_database_json.loadTable(serverName, "Test_Modes", "tests/data/modes.json")

if __name__ == '__main__':
  if (len(sys.argv) == 0):
    print "USAGE: %s serverName" % sys.argv[0]
  setupForTesting(sys.argv[1])
