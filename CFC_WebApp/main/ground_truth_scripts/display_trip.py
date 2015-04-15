__author__ = 'Yin'
from ast import literal_eval
import pygmaps_modified
import glob
import json

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

    if len(activities) > 1:
        activity_counter = 0
        for activity in activities:
            track_points = activity['trackPoints']
            if track_points != []:
                path = []
                point_counter = 0
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
                    gmap.addpoint(point['lat'], point['lon'], color, 'activity ' + str(activity_counter) + ', ' + str(point_counter))
                    point_counter += 1
                gmap.addpath(path, color)
    elif len(activities) == 1:
        activity_counter = 0
        for activity in activities:
            track_points = activity['trackPoints']
            if track_points != []:
                path = []
                point_counter = 0
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
                    gmap.addpoint(point['lat'], point['lon'], color, str(point_counter))
                    point_counter += 1
                gmap.addpath(path, color)        


def clean_section(section, remove_indices=None):
    cleaned_section = section.copy()
    for activity in remove_indices:
        original_points = cleaned_section['activities'][activity]['trackPoints']
        cleaned_points = []
        c = 0
        while c < len(original_points):
            if c not in remove_indices[activity]:
                cleaned_points.append(original_points[c])

            c += 1
        del cleaned_section['activities'][activity]['trackPoints'][:]

        for p in cleaned_points:
            cleaned_section['activities'][activity]['trackPoints'].append(p)

    return cleaned_section

def display_trip(file_name):
    with open(file_name, 'r') as f:
        data = f.readlines()
    sections = literal_eval(data[0])
    start = sections[0]['activities'][0]['trackPoints'][0]
    print(start)
    gmap = pygmaps_modified.maps(start['lat'], start['lon'], 14)
    for section in sections:
        # print(section.keys())
        if section['type'] == 'move':
            drawSection(section, gmap)
    gmap.draw('visualizations/' + file_name.lstrip('cleaned_data/') + '.html')

def clean_file(file_name, remove_indices):
    with open(file_name, 'r') as f:
        data = f.readlines()
    sections = literal_eval(data[0])
    print(type(sections))
    cleaned_sections = []
    for section in sections:
        if section['type'] == 'move':
            cleaned_section = clean_section(section, remove_indices)
            cleaned_sections.append(cleaned_section)

    f = open('cleaned_data/' + file_name.lstrip('data/'), 'w+')
    f.write(json.dumps(cleaned_sections))

#for filename in glob.glob("data/*"):
 #   display_trip(filename)
    #clean_section

clean_file('data/1422387864.6', remove_indices = {0: [12]})


display_trip('cleaned_data/1422387864.6')





