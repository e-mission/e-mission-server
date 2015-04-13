
from pykml import parser
from pykml.parser import Schema
from pykml.factory import KML_ElementMaker as KML
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


def validate_kml(filepath, schema = "https://developers.google.com/kml/schema/kml21.xsd"):
        """
        Validates kml located at filepath.
        TODO: Even imported kml files do not validate against default xml
        """
        schema_gomaps = Schema(schema)
	data = ""
	with open(filepath, "r") as temp:
		data = temp.read()
	assert(data != "")
	doc = parser.fromstring(data)
        return schema_ogc.assertValid(doc)

def section_to_kml(section, outfile_path="", write=True):
        """
        Converts a section(s) into a kml file
        """
        color = "1267FF"
        make_coord = lambda p: (",".join(map(lambda x: str(x), 
                         p["track_location"]["coordinates"]) + ["0.0"]))
        pm = KML.Placemark(
                KML.styleUrl("#line-1267FF-5"),
                KML.name(section['_id']),
                KML.LineString(
                        KML.tessellate(1),                        
                        KML.coordinates(" ".join(
                                map(lambda track_point: make_coord(track_point)
                                    ,section['track_points'][1:-1])))
                )
        )
        start_point = KML.Placemark(
                KML.name("Start"),
                KML.description("Starting point"),
                KML.Point(KML.coordinates(make_coord(section['track_points'][0])))
        )
        end_point = KML.Placemark(
                KML.name("End"),
                KML.description("Ending point"),
                KML.Point(KML.coordinates(make_coord(section['track_points'][-1])))
        )
        line_style = KML.Style(
                KML.LineStyle(
                        KML.color("FF6712"),
                        KML.width("5")
                )
        )
        line_style.set("id","#line-1267FF-5")
        fld = KML.Folder(
                KML.name(section['user_id']),
                pm,
                start_point,
                end_point,
                line_style
        )
        
        if write:                
                kml = KML.kml(KML.Document(fld))
                outfile = file(str(section['user_id'])+'.kml','w')
                outfile.write(etree.tostring(kml, pretty_print=True))
        else:
                print etree.tostring(fld, pretty_print=True)
                return fld

def sections_to_kml(user_id, sections, outfile_path=""):
        kml = KML.kml(KML.Document(*map(lambda section: section_to_kml(section, write=False) if len(section['track_points']) > 1 else None ,sections)))
        outfile = file(str(user_id)+'.kml','w')
        outfile.write(etree.tostring(kml, pretty_print=True))
        
if __name__ == "__main__":
        from lxml import etree
        import sys, os
        sys.path.append("%s/../" % os.getcwd())
        from get_database import get_section_db
        from uuid import UUID
        Sections = get_section_db()
        user_id = "__ID_GOES_HERE__"
        sections = Sections.find({'$and': [{'user_id':user_id}]})
        sections_to_kml(user_id,sections)
