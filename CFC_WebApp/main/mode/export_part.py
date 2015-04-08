from pykml import parser
from os import path
import json

sampleTrip = '{"date": "20140413", "lastUpdate": "20140414T064442Z", "segments": [], "summary": null}'
sampleSegment = '{"place": {"type": "unknown", "id": 54095848, "location": {"lat": 37.3910149202, "lon": -122.0865010796}}, "endTime": "20140413T234434-0700", "type": "place", "startTime": "20140412T190446-0700", "lastUpdate": "20140414T064442Z"}'

data = ""
with open("directions.kml", "r") as temp:
	data = temp.read()
assert(data != "")
doc = parser.fromstring(data)
dataPoints = doc.Document.Placemark.LineString.coordinates.text.split(" ")
coordinatePoints = [x.split(",")[:2] for x in dataPoints]

segmentsList = []
for coord in coordinatePoints:
	j = json.loads(sampleSegment)
	j["place"]["location"]["lat"] = coord[0] # 0th index might be longitude
	j["place"]["location"]["lon"] = coord[1] 
	segmentsList.append(j)

j = json.loads(sampleTrip)
j["segments"] = segmentsList
with open('outfile', 'w') as outfile:
	json.dump(j, outfile)