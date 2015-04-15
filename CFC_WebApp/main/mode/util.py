
from pykml import parser
from pykml.parser import Schema
from pykml.factory import KML_ElementMaker as KML
from os import path
import json
import geojson
from lxml import etree
import sys, os
from random import randrange


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
        color = "".join(['ff'] + [hex(randrange(255))[-2:] for i in range(3)])
        make_coord = lambda p: (",".join(map(lambda x: str(x), 
                         p["track_location"]["coordinates"]) + ["0.0"]))
        make_coord_point = lambda p: (",".join(map(lambda x: str(x), 
                         p["coordinates"]) + ["0.0"]))
	style_id = "style-%s" % section['section_start_time']
        pm = KML.Placemark(
                KML.styleUrl("#%s" % style_id),
                KML.name(section['_id']),
                KML.LineString(
                        KML.tessellate(1),                        
                        KML.coordinates(" ".join(
                                map(lambda track_point: make_coord(track_point)
                                    ,section['track_points'][1:-1])))
                )
        )
        start_point = KML.Placemark(
		KML.styleUrl("#%s" % style_id),
                KML.name("Start"),
                KML.description(section['section_start_time']),
                KML.Point(KML.coordinates(make_coord_point(section['section_start_point'])))
        )
        end_point = KML.Placemark(
		KML.styleUrl("#%s" % style_id),
                KML.name("End"),
                KML.description(section['section_end_time']),
                KML.Point(KML.coordinates(make_coord_point(section['section_end_point'])))
        )
        style = KML.Style(
                KML.LineStyle(
                        KML.color(color),
                        KML.width(3)
                ),
                KML.IconStyle(
                        KML.color(color),
			KML.scale(1.1),
			KML.Icon(
			   KML.href("http://www.gstatic.com/mapspro/images/stock/503-wht-blank_maps.png")
			)
                )
        )
        style.set("id",style_id)
        fld = KML.Folder(
                KML.name(section['_id']),
                pm,
                start_point,
                end_point
        )
        
        if write:                
                kml = KML.kml(KML.Document(fld))
                path = os.path.join(outfile_path, str(section['user_id']) +'.kml')
                outfile = file(path,'w')
                outfile.write(etree.tostring(kml, pretty_print=True))
        else:
                return (fld, style)

def sections_to_kml(filename, sections, outfile_path=""):
        # kml = KML.kml(KML.Document(*map(lambda section: section_to_kml(section, write=False) if len(section['track_points']) > 1 else None ,sections)))
	foldersAndStyles = map(lambda section: section_to_kml(section, write=False) if len(section['track_points']) > 1 else None ,sections)
	folders = [fs[0] for fs in foldersAndStyles]
	styles = [fs[1] for fs in foldersAndStyles]

        kml = KML.kml(KML.Document(*map(lambda fs: fs, folders + styles)))
        path = os.path.join(outfile_path, filename +'.kml')
        outfile = file(path,'w')
        outfile.write(etree.tostring(kml, pretty_print=True))


def chunks(l,n):
        """
        Generates evenly sized chunks of a list
        """
        for i in xrange(0, len(l), n):
                yield l[i:i+n]

if __name__ == "__main__":
        sys.path.append("%s/../" % os.getcwd())
        from get_database import get_section_db
        from uuid import UUID
        Sections = get_section_db()
        user_id = "__ID_GOES_HERE__"
        sections = Sections.find({'$and': [{'user_id':user_id}]})
        sections_to_kml(user_id,sections)
