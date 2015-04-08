from pykml import parser
from os import path
import json

# with open("test.kmz") as f:
# 	doc = parser.parse(f)
# 	print(doc)

data = ""
with open("directions.kml", "r") as temp:
	data = temp.read()
assert(data != "")
doc = parser.fromstring(data)
dataPoints = doc.Document.Placemark.LineString.coordinates.text.split(" ")
coordinatePoints = [x.split(",")[:2] for x in dataPoints]

sampleSegment = '{"place": {"type": "unknown", "id": 54095848, "location": {"lat": 37.3910149202, "lon": -122.0865010796}}, "endTime": "20140413T234434-0700", "type": "place", "startTime": "20140412T190446-0700", "lastUpdate": "20140414T064442Z"}'
segmentsList = []
for coord in coordinatePoints:
	j = json.loads(sampleSegment)
	print(sampleSegment["place"])
	segmentsList.append(j.text)