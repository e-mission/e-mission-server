from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import json
import sys
from get_database import get_section_db, get_db

def fixFormat(dataStr):
  dataStr = dataStr.replace("u'", "'")
  dataStr = dataStr.replace("'", '"')
  dataStr = dataStr.replace("False", "false")
  return dataStr

def loadJSON(fileName):
  fileHandle = open(fileName)
  dataStr = fileHandle.readline()
  dataStr = fixFormat(dataStr)
  dataJSON = json.loads(dataStr)
  return dataJSON

def loadData(serverName, fileName):
  Sections=get_section_db()
  dataJSON = json.load(open(fileName))
  for section in dataJSON:
    try:
      Sections.insert(section)
    except DuplicateKeyError:
      print "Duplicate key found while loading, skipping..."
      

def loadTable(serverName, tableName, fileName):
  tableColl = get_db()[tableName]
  dataJSON = json.load(open(fileName))
  for row in dataJSON:
    tableColl.insert(row)

if __name__ == '__main__':
    if (len(sys.argv) == 0):
      print ("USAGE: %s <serverName> <sectionsFileName>" % sys.argv[0])
    else:
      loadData(sys.argv[1], sys.argv[2])
      loadTable(sys.argv[1], 'Stage_Modes', 'CFC_WebApp/tests/data/modes.json')
