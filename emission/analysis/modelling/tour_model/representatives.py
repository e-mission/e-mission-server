# standard imports
import numpy
import math
import copy

# our imports
from emission.core.wrapper.trip import Trip, Coordinate

"""
This class creates a group of representatives for each cluster
and defines the locations that the user visits from those clusters.

The purpose of this class is to get the list of clusters with 
start and end points to create the tour graph.

To use this class, as input it takes
- data: A list of trip objects
- labels: A list of integers that define the clusters on the data. 
The labels are calculated in cluster pipeline from the clusters. The labels 
should be a list of integers of the same length as the list of data, where 
different numbers indicate different clusters. 
"""

class representatives:

    def __init__(self, data, labels):
        self.data = data
        if not self.data:
            self.data = []
        self.labels = labels
        if not self.labels:
            self.labels = []
        if len(self.data) != len(self.labels):
            raise ValueError('Length of data must equal length of clustering labels.')
        self.num_clusters = len(set(self.labels))
        self.size = len(self.data)

    #get the list of clusters based on the labels
    def list_clusters(self):
        if not self.data:
            self.clusters = []
            return
        self.clusters = [0] * self.num_clusters
        for i in range(self.num_clusters):
            self.clusters[i] = []
        for i in range(len(self.labels)):
            a = self.labels[i]
            self.clusters[a].append(self.data[i])

    #get the representatives for each cluster
    def get_reps(self):
        self.reps = []
        if not self.data:
            return
        for cluster in self.clusters:
            points = [[], [], [], []]
            for c in cluster:
                points[0].append(c.trip_start_location.lat)
                points[1].append(c.trip_start_location.lon)
                points[2].append(c.trip_end_location.lat)
                points[3].append(c.trip_end_location.lon)
            centers = numpy.mean(points, axis=1)
            a = Trip(None, None, None, None, None, None, Coordinate(centers[0], centers[1]), Coordinate(centers[2], centers[3]))
            self.reps.append(a)

    #map the representatives
    def map(self):
        import pygmaps
        mymap = pygmaps.maps(37.5, -122.32, 10)
        for t in self.reps:
            start_lat = t.trip_start_location.lat
            start_lon = t.trip_start_location.lon
            end_lat = t.trip_end_location.lat
            end_lon = t.trip_end_location.lon
            path = [(start_lat, start_lon), (end_lat, end_lon)]
            mymap.addpath(path)
        for l in self.locs:
            mymap.addpoint(l.lat, l.lon, '#0000FF')
        mymap.draw('./myreps.html')


    #define the set of locations for the data
    def locations(self):
        self.bins = []
        self.locs = []
        if not self.data:
            self.num_locations = 0
            return
        for a in range(self.num_clusters):
            added_start = False
            added_end = False
            for bin in self.bins:
                if self.match('start', a, bin) and not added_start:
                    bin.append(('start', a))
                    added_start = True
                if self.match('end', a, bin) and not added_end:
                    bin.append(('end', a))
                    added_end = True
            if not added_start:
                newbin = [('start', a)]
                if self.match('end', a, newbin) and not added_end:
                    newbin.append(('end', a))
                    added_end = True
                self.bins.append(newbin)
            if not added_end:
                self.bins.append([('end', a)])

        self.num_locations = len(self.bins)

        self.locs = []
        for bin in self.bins:
            locs = []
            for b in bin:
                if b[0] == 'start':
                    point = self.reps[b[1]].trip_start_location
                if b[0] == 'end':
                    point = self.reps[b[1]].trip_end_location
                locs.append([point.lat, point.lon])
            locs = numpy.mean(locs, axis=0)
            coord = Coordinate(locs[0], locs[1])
            self.locs.append(coord)

    #create the input to the tour graph
    def cluster_dict(self):
        self.tour_dict = [0] * self.num_clusters
        if not self.data:
            self.tour_dict = []
            self.self_loops_tour_dict = []
            return
        for i in range(self.num_clusters):
            a = {'sections' : self.clusters[i]}
            self.tour_dict[i] = a
        for i in range(self.num_locations):
            bin = self.bins[i]
            for b in bin:
                cluster = b[1]
                label = b[0]
                self.tour_dict[cluster][label] = i
        for i in range(self.num_clusters):
            cluster = self.tour_dict[i]
            start_coords = self.locs[cluster['start']]
            end_coords = self.locs[cluster['end']]
            self.tour_dict[i]['start_coords'] = start_coords
            self.tour_dict[i]['end_coords'] = end_coords

        self.self_loops_tour_dict = copy.deepcopy(self.tour_dict)        

        for i in range(len(self.tour_dict)-1, -1, -1):
            cluster = self.tour_dict[i]
            if cluster['start'] == cluster['end']:
                self.tour_dict.remove(cluster)

        newlocs = []
        for cluster in self.tour_dict:
            if cluster['start'] not in newlocs:
                newlocs.append(cluster['start'])
            if cluster['end'] not in newlocs:
                newlocs.append(cluster['end'])
        for i in range(len(self.tour_dict)):
            self.tour_dict[i]['start'] = newlocs.index(self.tour_dict[i]['start'])
            self.tour_dict[i]['end'] = newlocs.index(self.tour_dict[i]['end'])

    #check whether a point is close to all points in a bin
    def match(self, label, a, bin):
        if label == 'start':
            pointa = self.reps[a].trip_start_location
        elif label == 'end':
            pointa = self.reps[a].trip_end_location
        for b in bin:
            if b[0] == 'start':
                pointb = self.reps[b[1]].trip_start_location
            elif b[0] == 'end':
                pointb = self.reps[b[1]].trip_end_location
            if self.distance(pointa.lat, pointa.lon, pointb.lat, pointb.lon) > 300:
                return False
        return True

    #the meter distance between two points
    def distance(self, lat1, lon1, lat2, lon2):
        R = 6371000
        rlat1 = math.radians(lat1)
        rlat2 = math.radians(lat2)
        lon = math.radians(lon2 - lon1);
        lat = math.radians(lat2-lat1);
        a = math.sin(lat/2.0)**2 + math.cos(rlat1)*math.cos(rlat2) * math.sin(lon/2.0)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = R * c
        return d
