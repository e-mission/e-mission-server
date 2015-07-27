import numpy
import math
import matplotlib.pyplot as plt
#import pygmaps
from matplotlib import colors as matcol
from emission.core.wrapper.trip import Trip, Coordinate

"""
This class creates a group of representatives for each cluster
and defines the locations that the user visits from those clusters.

The purpose of this class is to get the list of clusters with 
start and end points to create the tour graph.

To use this class, as input it takes
- data: the data that's clustered. Should be in the same format as the 
section database data. 
- labels: a list of integers that define the clusters on the data.

"""

class representatives:

    def __init__(self, data, labels):
        self.data = data
        self.labels = labels
        if not data or not labels:
            raise ValueError('Both data and labels must not be empty')
        if len(data) != len(labels):
            raise ValueError('Length of data must equal length of clustering labels.')
        self.num_clusters = len(set(labels))
        self.size = len(data)

    #get the list of clusters based on the labels
    def list_clusters(self):
        self.clusters = [0] * self.num_clusters
        for i in range(self.num_clusters):
            self.clusters[i] = []
        for i in range(len(self.labels)):
            a = self.labels[i]
            self.clusters[a].append(self.data[i])

    #get the representatives for each cluster
    def reps(self):
        self.reps = []
        for cluster in self.clusters:
            points = [[]]*4
            for c in cluster:
                points[0].append(c.trip_start_location.lat)
                points[1].append(c.trip_start_location.lon)
                points[2].append(c.trip_end_location.lat)
                points[3].append(c.trip_end_location.lon)
            centers = numpy.mean(points, axis=0)
            a = Trip(None, None, None, None, None, None, Coordinate(centers[0], centers[1]), Coordinate(centers[2], centers[3]))
            #a = {'section_start_po' : {'coordinates' : [centers[1], centers.lon]}, 'section_end_point' : {'coordinates' : [centers[3], centers[2]]}}
            self.reps.append(a)

    #define the set of locations for the data
    def locations(self):
        self.bins = []
        for a in range(self.num_clusters):
            added_start = False
            added_end = False
            for bin in self.bins:
                if self.match('start', a, bin):
                    bin.append(('start', a))
                    added_start = True
                if self.match('end', a, bin):
                    bin.append(('end', a))
                    added_end = True
            if not added_start:
                newbin = [('start', a)]
                if self.match('end', a, newbin):
                    newbin.append(('end', a))
                    added_end = True
                self.bins.append(newbin)
            if not added_end:
                self.bins.append([('end', a)])

        self.num_locations = len(self.bins)

    #create the input to the tour graph
    def cluster_dict(self):
        self.tour_dict = [0] * self.num_clusters
        for i in range(self.num_clusters):
            a = {'sections' : self.clusters[i]}
            self.tour_dict[i] = a
        for i in range(self.num_locations):
            bin = self.bins[i]
            for b in bin:
                cluster = b[1]
                label = b[0]
                self.tour_dict[cluster][label] = i

    #check whether a point is close to all points in a bin
    def match(self, label, a, bin):
        if label == 'start':
            pointa = self.reps[a].trip_start_location
        elif label == 'end':
            pointa = self.reps[a].trip_end_location
        for b in bin:
            if b[0] == 'start':
                pointb = self.reps[b[1]].trip_start_location
            else:
                pointb = self.reps[b[1]].trip_end_location
            if not self.distance(pointa.lat, pointa.lon, pointb.lat, pointb.lon):
                return False
        return True

    #tour graph visualization
    def graph(self):
        import networkx as nx
        import datetime
        G = nx.DiGraph()
        for v in range(self.num_locations):
            G.add_node(v)
        for e in self.tour_dict:
            a = e['start']
            b = e['end']
            G.add_edge(a,b,days=set(),times=set())
            for s in e['sections']:
                date = s['section_start_datetime']
                G[a][b]['days'].add(date.isoweekday())
                G[a][b]['times'].add(date.hour)
        nx.draw_random(G)
        plt.suptitle('Tour Graph')
        plt.savefig('tourgraph.png')
        plt.show()
        plt.clf()

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
        if d <= 300:
            return True
        return False



    def mapping(self):
        for bin in self.bins:
            first = True
            for b in bin:
                start_lat = self.reps[b[1]].trip_start_location.lat
                start_lon = self.reps[b[1]].trip_start_location.lon
                end_lat = self.reps[b[1]].trip_end_location.lat
                end_lon = self.reps[b[1]].trip_end_location.lon
                if first:
                    mymap = pygmaps.maps(start_lat, start_lon, 10)
                    first = False
                path = [(start_lat, start_lon), (end_lat, end_lon)]
                mymap.addpath(path)
            mymap.draw('./mybins' + str(self.bins.index(bin)) + '.html')

        mymap = pygmaps.maps(37.5, -122.32, 10)
        for rep in self.reps:
            start_lat = rep.trip_start_location.lat
            start_lon = rep.trip_start_location.lon
            end_lat = rep.trip_end_location.lat
            end_lon = rep.trip_end_location.lon
            path = [(start_lat, start_lon), (end_lat, end_lon)]
            mymap.addpath(path)
        mymap.draw('./myreps.html')

        import pygmaps
        self.locations = []
        for bin in self.bins:
            mymap = pygmaps.maps(37.5, -122.32, 10)
            locs = []
            for b in bin:
                if b[0] == 'start':
                    point = self.reps[b[1]].trip_start_location
                if b[0] == 'end':
                    point = self.reps[b[1]].trip_end_location
                locs.append(point)
                mymap.addpoint(point[1], point[0], '#FF0000')
            locs = numpy.mean(locs, axis=0)
            mymap.addpoint(locs[1], locs[0], '#0000FF')
            self.locations.append([locs[0], locs[1]])
            mymap.draw('./mylocs' + str(self.bins.index(bin)) + '.html')

        colormap = plt.cm.get_cmap()
        for i in range(self.num_locations):
            mymap = pygmaps.maps(37.5, -122.32, 10)
            for cluster in self.tour_dict:
                if cluster['start'] == i or cluster['end'] == i:
                    for c in cluster['sections']:
                        start = c.trip_start_location
                        end = c.trip_end_location
                        path = [(start.lat, start.lon), (end.lat, end.lon)]
                        mymap.addpath(path)
            mymap.draw('./mytourdict' + str(i) + '.html')

