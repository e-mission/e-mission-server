
from pykml import parser
from pykml.parser import Schema
from pykml.factory import KML_ElementMaker as KML
from os import path
import json
import geojson
from lxml import etree
import sys, os, random
from datetime import datetime
from random import randrange


sampleTrip = '{"date": "20140413", "lastUpdate": "20140414T064442Z", "segments": [], "summary": null}'
sampleSegment = '{"place": {"type": "unknown", "id": 54095848, "location": {"lat": 37.3910149202, "lon": -122.0865010796}}, "endTime": "20140413T234434-0700", "type": "place", "startTime": "20140412T190446-0700", "lastUpdate": "20140414T064442Z"}'


def kml_multiple_to_geojson(infile_path, outdir_path, geojson_properties={}):
        """
        Converts a KML file with multiple Documents into geojson to store in an output file
        """
        data = __read_file(infile_path)
        coord_dict = __get_all_coords(data)
        if not os.path.exists(outdir_path):
                os.makedirs(outdir_path)        
        for section_id, coords in coord_dict.items():
                filename = "%s.json" % section_id
                path = os.path.join(outdir_path, filename)
                outfile = file(path,'w')
                dump = __to_geojson(coords)
                outfile.write(dump)
                outfile.close()

def kml_to_geojson(infile_path, outfile_path, geojson_properties={}):
	"""
	Converts a KML file to geojson to store in an output file.
	"""
        data = __read_file(infile_path)  
      	coords = __get_coords(data)
	outfile = open(outfile_path, 'w')
	outfile.write(__to_geojson(coords))
	outfile.close()

def __to_geojson(coordinates, geojson_properties={}):
        coordinatePoints = geojson.MultiPoint(coordinates)
	dump = geojson.dumps(geojson.Feature(geometry=coordinatePoints, properties=geojson_properties)) # can add properties and id to feature (perhaps trip/section id?)
        return dump

def kml_to_json(infile_path, outfile_path):
	"""
	Converts a KML file to geojson to store in an output file.
	"""
        data = __read_file(infile_path)
	coordinatePoints = __get_coords(data)
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

def __read_file(infile_path):
        data = ""
	with open(infile_path, "r") as temp:
		data = temp.read()
	assert(data != "")
        return data

def __get_coords(data):	
	doc = parser.fromstring(data)
	dataPoints = doc.Document.Placemark.LineString.coordinates.text.split(" ") 
	return [[float(i) for i in x.split(",")[:2]] for x in dataPoints]

def __get_all_coords(data):
        doc = parser.fromstring(data) 
        coords = {}     
        for folder in doc.Document.getchildren():
               	dataPoints = folder.Placemark.LineString.coordinates.text.split(" ") 
                section_id = folder.Placemark.name.text
                coords[section_id] = [[float(i) for i in x.split(",")[:2]] for x in dataPoints]
        return coords
        
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

def section_to_kml(section, color, outfile_path="", write=True):
        """
        Converts a section into a kml file
        """
        line_style_id = "line-%s-5" % color
        red = "FF1212"
        green = "00B80C"
        start_icon_style_id = "icon-%s" % color
        end_icon_style_id = "icon-%s" % color        
        make_coord = lambda p: (",".join(map(lambda x: str(x), 
                         p["track_location"]["coordinates"]) + ["0.0"]))
        make_coord_point = lambda p: (",".join(map(lambda x: str(x), 
                         p["coordinates"]) + ["0.0"]))
	style_id = "style-%s" % section['section_start_time']
        pm = KML.Placemark(
                KML.styleUrl("#%s" % line_style_id),
                KML.name(section['_id']),
                KML.LineString(
                        KML.tessellate(1),                        
                        KML.coordinates(" ".join(
                                map(lambda track_point: make_coord(track_point)
                                    ,section['track_points'])))
                )
        )
        start_point = section['section_start_point']
        end_point = section['section_end_point']
        start_time = mongodate_to_datetime(section["section_start_time"])
        end_time = mongodate_to_datetime(section["section_end_time"])
        start_point = KML.Placemark(
                KML.styleUrl("#%s" % start_icon_style_id),                
                KML.name("Start: %s" % start_time),
                KML.description("Starting point"),
                KML.Point(KML.coordinates(make_coord_point(start_point)))
        )
        end_point = KML.Placemark(
                KML.styleUrl("#%s" % end_icon_style_id),
                KML.name("End: %s" % end_time),
                KML.description("Ending point"),
                KML.Point(KML.coordinates(make_coord_point(end_point)))
        )
        line_style = KML.Style(
                KML.LineStyle(
                        KML.color("ff%s" % color),
                        KML.width("5")
                )
        )
        line_style.set("id", line_style_id)
        start_icon_style = KML.Style(
                KML.IconStyle(
                        KML.color("ff%s" % color),
                        KML.scale("1.1"),
                        KML.Icon(
                                KML.href("http://www.gstatic.com/mapspro/images/stock/503-wht-blank_maps.png")
                        )
                )
        )
        start_icon_style.set("id", start_icon_style_id)
        end_icon_style = KML.Style(
                KML.IconStyle(
                        KML.color("ff%s" % color),
                        KML.scale("1.1"),
                        KML.Icon(
                                KML.href("http://www.gstatic.com/mapspro/images/stock/503-wht-blank_maps.png")
                        )                       
                )
        )
        end_icon_style.set("id", end_icon_style_id)
        fld = KML.Folder(
                KML.name(section['_id']),
                KML.description("From %s \nto %s" % (start_time, end_time)),
                pm,
                start_point,
                end_point
        )       
        if write:                
                kml = KML.kml(KML.Document(fld, section["user_id"]))
                path = os.path.join(outfile_path, str(section['user_id']) +'.kml')
                outfile = file(path,'w')
                outfile.write(etree.tostring(kml, pretty_print=True))
        else:
                return fld, line_style, start_icon_style, end_icon_style

def sections_to_kml(filename, sections, outfile_path=""):
        r = lambda: random.randint(0,255)
        l_tuples = map(lambda section: section_to_kml(section, '%02X%02X%02X' % (r(),r(),r()),  write=False) if len(section['track_points']) > 1 else None ,sections)
        flat = []
        
        for f,s,i_s,i_e in l_tuples:
                flat.append(f)
                flat.append(s)
                flat.append(i_s)
                flat.append(i_e)
        kml = KML.kml(KML.Document(sections[0]["user_id"],*flat))
        path = os.path.join(outfile_path, filename +'.kml')
        outfile = file(path,'w')
        outfile.write(etree.tostring(kml, pretty_print=True))

def chunks(l,n):
        """
        Generates evenly sized chunks of a list
        """
        for i in xrange(0, len(l), n):
                yield l[i:i+n]

def mongodate_to_datetime(date):
        return datetime.strptime(date[:-5], "%Y%m%dT%H%M%S")
        

if __name__ == "__main__":
        sys.path.append("%s/../" % os.getcwd())
        from get_database import get_section_db
        from uuid import UUID
        Sections = get_section_db()
        user_id = "__ID_GOES_HERE__"
        sections = Sections.find({'$and': [{'user_id':user_id}]})
        sections_to_kml(user_id,sections)
