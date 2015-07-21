import json,httplib
connection = httplib.HTTPSConnection('api.parse.com', 443)
connection.connect()
connection.request('POST', '/1/push',
    json.dumps({
       "where": {
         "deviceType": "ios"
       },
       "data": {
         # "alert": "The Mets scored! The game is now tied 1-1.",
         "content-available": 1,
         "sound": "",
       }
    }), {
       "X-Parse-Application-Id": "ThFx7FEFNfQ48ODji1quVe3mwnvNjSDl6JxIFxs4",
       "X-Parse-REST-API-Key": "9lcyFxgoHvfr4wDvJ6NSm506wV2PbXKUs0dnE5gk",
       "Content-Type": "application/json"
    })
result = json.loads(connection.getresponse().read())
print result
