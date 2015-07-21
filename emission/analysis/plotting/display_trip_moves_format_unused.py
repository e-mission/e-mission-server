__author__ = 'Yin'
from ast import literal_eval
import pygmaps

POINTS = 'points'
PATH = 'path'
ALL = 'all'
COLOR = {1:"#0000FF",       #walking - blue
         2:"#00FF00",       #running - green
         3:"#FFFF00",       #cycling - yellow
         4:"#FF0000",       #transport - red
         5:"#00FFFF",       #bus - aqua
         6:"#FF8C00",       #train - darkOrange
         7:"#778899",       #car - grey
         8:"#808000",       #mixed - olive
         9:"#87CEFA"        #air - skyBlue
        }

def drawSection(section, gmap):
    activities = section['activities']
    for activity in activities:
        track_points = activity['trackPoints']
        #track_points = activity['track_points']
        if track_points != []:
            path = []
            for point in track_points:
                coordinate_tuple = tuple([point['lat'], point['lon']])
                acc = point['accuracy']
                if acc <= 30:
                    color = COLOR[7]
                if acc > 30 and acc <= 60:
                    color = COLOR[1]
                if acc > 60 and acc <= 90:
                    color = COLOR[2]
                if acc > 90 and acc <= 120:
                    color = COLOR[3]
                if acc > 120:
                    color = COLOR[4]
                path.append(coordinate_tuple)
                gmap.addpoint(point['lat'], point['lon'], color)
            gmap.addpath(path, color)

def display_trip(file_name):
    with open(file_name, 'r') as f:
        data = f.readlines()
    sections = literal_eval(data[0])
    gmap = pygmaps.maps(37.8717, -122.2728, 14)
    for section in sections:
        print(section.keys())
        if section['type'] == 'move':
            drawSection(section, gmap)
            gmap.draw(file_name + '.html')


display_trip("sensed_trips.json")
