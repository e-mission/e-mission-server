# Standard imports
import math
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy
from sklearn import metrics
import sys
from scipy.optimize import curve_fit
from numpy import cross
from numpy.linalg import norm
"""
This class organizes data into bins by similarity. It then orders the bins 
by largest to smallest and removes the bottom portion of the bins. 

Two trips are in the same bin if both their start points and end points 
are within a certain number of meters of each others. 

As input, this class takes the following:
- data: the data to put into bins. The data should be a list of Trip objects that have 
start and end locations. 
- radius: the radius for determining how close the start points and end points of two 
trips have to be for the trips to be put in the same bin

This is called by cluster_pipeline.py.
"""
class similarity:
    
    def __init__(self, data, radius):
        self.data = data
        if not data:
            self.data = []
        self.bins = []
        self.radius = float(radius)
        for a in range(len(self.data)-1, -1, -1):
            start_lat = self.data[a].trip_start_location.lat
            start_lon = self.data[a].trip_start_location.lon
            end_lat = self.data[a].trip_end_location.lat
            end_lon = self.data[a].trip_end_location.lon
            if self.distance(start_lat, start_lon, end_lat, end_lon):
                self.data.pop(a)
        print 'After removing trips that are points, there are ' + str(len(self.data)) + ' data points'
        self.size = len(self.data)

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
        if len(self.bins) <= 1:
            return
        num = self.elbow_distance()
        for i in range(len(self.bins)):
            if len(self.bins[i]) <= len(self.bins[num]):
                num = i
                break
        print 'the cutoff point is ' + str(num)
        self.num = num
        self.graph()
        for i in range(len(self.bins) - num):
            self.bins.pop()
        newdata = []
        for bin in self.bins:
            for b in bin:
                d = self.data[b]
                newdata.append(self.data[b])
        self.newdata = newdata

    #calculate the cut-off point in the histogram
    def elbow_distance(self):
        y = [0] * len(self.bins)
        for i in range(len(self.bins)):
            y[i] = len(self.bins[i])
        N = len(y)
        x = range(N)
        max = 0
        index = -1
        a = numpy.array([x[0], y[0]])
        b = numpy.array([x[-1], y[-1]])
        n = norm(b-a)
        new_y = []
        for i in range(0, N):
            p = numpy.array([x[i], y[i]])
            dist = norm(numpy.cross(p-a,p-b))/n
            new_y.append(dist)
            if dist > max:
                max = dist
                index = i
        return index

    #check if two trips match
    def match(self,a,bin):
        for b in bin:
            if not self.distance_helper(a,b):
                return False
        return True

    #create the histogram
    def graph(self):
        bars = [0] * len(self.bins)
        for i in range(len(self.bins)):
            bars[i] = len(self.bins[i])
        N = len(bars)
        index = numpy.arange(N)
        width = .2
        plt.bar(index+width, bars, color='k')
        try: 
            plt.bar(self.num+width, bars[self.num], color='g')
        except Exception:
            pass
        plt.xlim([0, N])
        plt.xlabel('Bins')
        plt.ylabel('Number of elements')
        plt.savefig('histogram.png')

    #evaluate the bins as if they were a clustering on the data
    def evaluate_bins(self):
        self.labels = []
        for bin in self.bins:
            for b in bin:
                self.labels.append(self.bins.index(bin))
        if not self.data or not self.bins:
            return
        if len(self.labels) < 2:
            print 'Everything is in one bin.'
            return
        labels = numpy.array(self.labels)
        points = []
        for bin in self.bins:
            for b in bin:
                start_lat = self.data[b].trip_start_location.lat
                start_lon = self.data[b].trip_start_location.lon
                end_lat = self.data[b].trip_end_location.lat
                end_lon = self.data[b].trip_end_location.lon
                path = [start_lat, start_lon, end_lat, end_lon]
                points.append(path)
        a = metrics.silhouette_score(numpy.array(points), labels)
        print 'number of bins is ' + str(len(self.bins))
        print 'silhouette score is ' + str(a)
        return a

    #calculate the distance between two trips
    def distance_helper(self, a, b):
        starta = self.data[a].trip_start_location
        startb = self.data[b].trip_start_location
        enda = self.data[a].trip_end_location
        endb = self.data[b].trip_end_location

        start = self.distance(starta.lat, starta.lon, startb.lat, startb.lon)
        end = self.distance(enda.lat, enda.lon, endb.lat, endb.lon)
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
