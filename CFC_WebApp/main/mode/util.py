from pykml import parser
from os import path
import json
import geojson

sampleTrip = '{"date": "20140413", "lastUpdate": "20140414T064442Z", "segments": [], "summary": null}'
sampleSegment = '{"place": {"type": "unknown", "id": 54095848, "location": {"lat": 37.3910149202, "lon": -122.0865010796}}, "endTime": "20140413T234434-0700", "type": "place", "startTime": "20140412T190446-0700", "lastUpdate": "20140414T064442Z"}'

def kml_to_geojson(infile_path, outfile_path, geojson_properties={}):
	"""
	Converts a KML file to geojson to store in an output file.
	"""
	coordinatePoints = geojson.MultiPoint(__get_coords(infile_path))
	dump = geojson.dumps(geojson.Feature(geometry=coordinatePoints, properties=geojson_properties)) # can add properties and id to feature (perhaps trip/section id?)
	outfile = open(outfile_path, 'w')
	outfile.write(dump)
	outfile.close()

def kml_to_json(infile_path, outfile_path):
	"""
	Converts a KML file to geojson to store in an output file.
	"""
	coordinatePoints = __get_coords(infile_path)
	segmentsList = []
	for coord in coordinatePoints:
		j = json.loads(sampleSegment)
		j["place"]["location"]["lat"] = coord[0] # 0th index might be longitude
		j["place"]["location"]["lon"] = coord[1] 
		segmentsList.append(j)
	j = json.loads(sampleTrip)
	j["segments"] = segmentsList
	with open(outfile_path, 'w') as outfile:
		json.dump(j, outfile)

def __get_coords(infile_path):
	data = ""
	with open(infile_path, "r") as temp:
		data = temp.read()
	assert(data != "")
	doc = parser.fromstring(data)
	dataPoints = doc.Document.Placemark.LineString.coordinates.text.split(" ")
	return [[float(i) for i in x.split(",")[:2]] for x in dataPoints]