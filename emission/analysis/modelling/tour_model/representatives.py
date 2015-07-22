import numpy
import math

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
                points[0].append(c['section_start_point']['coordinates'][1])
                points[1].append(c['section_start_point']['coordinates'][0])
                points[2].append(c['section_end_point']['coordinates'][1])
                points[3].append(c['section_end_point']['coordinates'][0])
            centers = numpy.mean(points, axis=0)
            a = {'section_start_point' : {'coordinates' : [centers[1], centers[0]]}, 'section_end_point' : {'coordinates' : [centers[3], centers[2]]}}
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
                    break
                if self.match('end', a, bin):
                    bin.append(('end', a))
                    added_end = True
                    break
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
            self.tour_dict[i] = (a)
        for i in range(self.num_locations):
            bin = self.bins[i]
            for b in bin:
                cluster = b[1]
                label = b[0]
                self.tour_dict[cluster][label] = i

    #check whether a point is close to all points in a bin
    def match(self, label, a, bin):
        if label == 'start':
            pointa = self.reps[a]['section_start_point']['coordinates']
        elif label == 'end':
            pointa = self.reps[a]['section_end_point']['coordinates']
        for b in bin:
            if b[0] == 'start':
                pointb = self.reps[b[1]]['section_start_point']['coordinates']
            else:
                pointb = self.reps[b[1]]['section_end_point']['coordinates']
            if not self.distance(pointa[1], pointa[0], pointb[1], pointb[0]):
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
        if d <= 200:
            return True
        return False



