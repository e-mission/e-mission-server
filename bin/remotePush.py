import json,httplib
import sys

config_data = json.load(open('conf/net/ext_service/parse.json'))

interval = "interval_%s" % sys.argv[1]
print "pushing for interval %s" % interval

silent_push_msg = {
   "channels": [
     interval
   ],
   "data": {
     # "alert": "The Mets scored! The game is now tied 1-1.",
     "content-available": 1,
     "sound": "",
   }
}

# Sending messages to open source parse server
# This is the long-term one used while going forward
parse_headers = {
   "X-Parse-Application-Id": config_data["emission_id"],
   "X-Parse-REST-API-Key": config_data["emission_key"],
   "X-Parse-Master-Key": config_data["emission_master_key"],
   "Content-Type": "application/json"
}

connection = httplib.HTTPSConnection('parseapi.back4app.com', 443)
connection.connect()

connection.request('POST', '/push', json.dumps(silent_push_msg), parse_headers)

result = json.loads(connection.getresponse().read())
print "open parse server %s" % result
