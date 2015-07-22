# Standard imports
import math
import matplotlib.pyplot as plt
import numpy
from sklearn import metrics
from sklearn.metrics.cluster import homogeneity_score, completeness_score
import sys

"""
This class organizes data into bins by similarity. It then orders the bins 
by largest to smallest and removes the bottom portion of the bins. 

Two trips are in the same bin if both their start points and end points 
are within a certain number of meters of each others. 

As input, this class takes the following:
- data: the data to put into bins. The data should be a list of trips, with 
each trip as a dictionary containing a 'trip_start_location' and a 'trip_end_location'.
- percent: the percent of the bins to keep and return, after bins have been ordered
- radius: the radius for determining how close the start points and end points of two 
trips have to be for the trips to be put in the same bin
- colors (optional): the ground truth, in the form of a list of integers where different integers 
indicate different ground truth clusters. This is optional, and only for when ground truth exists
for evaluating how well the data is 'clustered' into bins. 

This is called by cluster_pipeline.py.
"""
class similarity:
    
    def __init__(self, data, percent, radius, colors=None):
        self.data = data
        self.size = len(self.data)
        self.percent = float(percent)
        if self.percent > 1:
            sys.stderr.write('Percent must be less than or equal to 1.\n')
            self.percent = 1.0
        if self.percent < 0:
            sys.stderr.write('Percent must be greater than or equal to 0.\n')
            self.percent = 0.0
        self.bins = []
        self.radius = float(radius)
        self.colors = colors

    #create bins
    def bin_data(self):
        for a in range(self.size):
            added = False
            for bin in self.bins:
                if self.match(a,bin):
                    bin.append(a)
                    added = True
                    break
            if not added:
                self.bins.append([a])
        self.bins.sort(key=lambda bin: len(bin), reverse=True)

    #delete lower portion of bins
    def delete_bins(self):
        num = int(math.ceil(len(self.bins) * self.percent))
        for i in range(len(self.bins) - num):
            self.bins.pop()


    #check if two trips match
    def match(self,a,bin):
        for b in bin:
            if not self.distance_helper(a,b):
                return False
        return True

    #create the histogram
    def graph(self):
        bars = [0] * len(self.bins)
        sum = 0
        for i in range(len(self.bins)):
            bars[i] = len(self.bins[i])
            if i < math.ceil(len(self.bins) * self.percent):
                sum += bars[i]
        N = len(bars)
        index = numpy.arange(N)
        width = .1
        plt.bar(index+width, bars, color='m')
        plt.xlim([0, N])
        print str(N) + ' bins, in top half of bins there are ' + str(sum) + ' items out of ' + str(self.size) 
        plt.savefig('histogram.png')

    #plot the trips on a map, with different colors
    #indicating different bins
    def map_bins(self):
        import pygmaps
        from matplotlib import colors as matcol
        colormap = plt.cm.get_cmap()
        mymap = pygmaps.maps(37.5, -122.32, 10)
        for bin in self.bins:
            for b in bin:
                start_lat = self.data[b]['trip_start_location'][1]
                start_lon = self.data[b]['trip_start_location'][0]
                end_lat = self.data[b]['trip_end_location'][1]
                end_lon = self.data[b]['trip_end_location'][0]
                path = [(start_lat, start_lon), (end_lat, end_lon)]
                mymap.addpath(path, matcol.rgb2hex(colormap(float(self.bins.index(bin))/len(self.bins))))
        mymap.draw('./mybins.html')
        
    #evaluate the bins as if they were a clustering on the data
    def evaluate_bins(self):
        self.labels = []
        newcolors = []
        for bin in self.bins:
            for b in bin:
                self.labels.append(self.bins.index(bin))
                if bool(self.colors):
                    newcolors.append(self.colors[b])
        self.colors = newcolors
                    
        labels = numpy.array(self.labels)
        colors = numpy.array(self.colors)
        points = []
        for bin in self.bins:
            for b in bin:
                start_lat = self.data[b]['trip_start_location'][1]
                start_lon = self.data[b]['trip_start_location'][0]
                end_lat = self.data[b]['trip_end_location'][1]
                end_lon = self.data[b]['trip_end_location'][0]
                path = [start_lat, start_lon, end_lat, end_lon]
                points.append(path)

        if bool(self.colors):
            a = metrics.silhouette_score(numpy.array(points), labels)
            b = homogeneity_score(colors, labels)
            c = completeness_score(colors, labels)
            
            print 'number of bins is ' + str(len(self.bins))
            print 'silhouette score is ' + str(a)
            print 'homogeneity is ' + str(b)
            print 'completeness is ' + str(c)
            print 'accuracy is ' + str(((a+1)/2.0 + b + c)/3.0)

    #update the ground truth
    def update_colors(self, newcolors):
        if bool(self.colors):
            indices = [] * len(set(newcolors))
            for n in newcolors:
                if n not in indices:
                    indices.append(n)
            for i in range(len(newcolors)):
                newcolors[i] = indices.index(newcolors[i])
            self.colors = newcolors


    #calculate the distance between two trips
    def distance_helper(self, a, b):
        starta = self.data[a]['section_start_point']['coordinates']
        startb = self.data[b]['section_start_point']['coordinates']
        enda = self.data[a]['section_end_point']['coordinates']
        endb = self.data[b]['section_end_point']['coordinates']

        start = self.distance(starta[1], starta[0], startb[1], startb[0])
        end = self.distance(enda[1], enda[0], endb[1], endb[0])
        if start and end:
            return True
        return False

    #calculate the meter distance between two trips
    def distance(self, lat1, lon1, lat2, lon2):
        R = 6371000
        rlat1 = math.radians(lat1)
        rlat2 = math.radians(lat2)
        lon = math.radians(lon2 - lon1);
        lat = math.radians(lat2-lat1);
        a = math.sin(lat/2.0)**2 + math.cos(rlat1)*math.cos(rlat2) * math.sin(lon/2.0)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = R * c
        if d <= self.radius:
            return True
        return False
