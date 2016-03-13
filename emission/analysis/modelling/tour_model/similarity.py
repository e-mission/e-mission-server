# Standard imports
import logging
import math
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy
from sklearn import metrics
import sys
from numpy import cross
from numpy.linalg import norm
import emission.storage.decorations.trip_queries as esdtq
import emission.storage.decorations.section_queries as esdsq

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
    
    def __init__(self, data, radius, old=True):
        self.data = data
        if not data:
            self.data = []
        self.bins = []
        self.radius = float(radius)
        self.old = old
        if not old:
            for a in self.data:
                # print "a is %s" % a
                t = esdtq.get_trip(a)
                try:
                    start_lon = t.start_loc["coordinates"][0]
                    start_lat = t.start_loc["coordinates"][1]
                    end_lon = t.end_loc["coordinates"][0]
                    end_lat = t.end_loc["coordinates"][1]
                    # logging.debug("start lat = %s" % start_lat)
                    if self.distance(start_lat, start_lon, end_lat, end_lon):
                        self.data.remove(a)
                except:
                    self.data.remove(a)
        else:
            for a in range(len(self.data)-1, -1, -1):
                start_lat = self.data[a].trip_start_location.lat
                start_lon = self.data[a].trip_start_location.lon
                end_lat = self.data[a].trip_end_location.lat
                end_lon = self.data[a].trip_end_location.lon
                if self.distance(start_lat, start_lon, end_lat, end_lon):
                    self.data.pop(a)

        logging.debug('After removing trips that are points, there are %s data points' % len(self.data))
        self.size = len(self.data)

    #create bins
    def bin_data(self):
        for a in range(self.size):
            added = False
            for bin in self.bins:
                try:
                    if self.match(a,bin):
                        bin.append(a)
                        added = True
                        break
                except:
                    added = False
            if not added:
                self.bins.append([a])
        self.bins.sort(key=lambda bin: len(bin), reverse=True)

    #delete lower portion of bins
    def delete_bins(self):
        if len(self.bins) <= 1:
            return
        num = self.elbow_distance()
        sum = 0
        for i in range(len(self.bins)):
            sum += len(self.bins[i])
            if len(self.bins[i]) <= len(self.bins[num]):
                sum -= len(self.bins[i])
                num = i
                break
        logging.debug('the new number of trips is %d' % sum)
        logging.debug('the cutoff point is %d' % num)
        self.num = num
        #self.graph()
        for i in range(len(self.bins) - num):
            self.bins.pop()
        newdata = []
        for bin in self.bins:
            for b in bin:
                d = self.data[b]
                newdata.append(self.data[b])
        self.newdata = newdata if len(newdata) > 1 else self.data


    #calculate the cut-off point in the histogram
    #This is motivated by the need to calculate the cut-off point 
    #that separates the common trips from the infrequent trips. 
    #This works by approximating the point of maximum curvature 
    #from the curve formed by the points of the histogram. Since 
    #it is a discrete set of points, we calculate the point of maximum
    #distance from the line formed by connecting the height of the 
    #tallest bin with that of the shortest bin, as described
    #here: http://stackoverflow.com/questions/2018178/finding-the-best-trade-off-point-on-a-curve?lq=1
    #We then remove all bins of lesser height than the one chosen.
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
            if not self.old:
                if not self.distance_helper_new(a,b):
                    return False
            else:
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
            logging.debug('Everything is in one bin.')
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
        logging.debug('number of bins is %d' % len(self.bins))
        logging.debug('silhouette score is %d' % a)
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

    def distance_helper_new(self, a, b):
        tripa = esdtq.get_trip(self.data[a])
        tripb = esdtq.get_trip(self.data[b])

        starta = tripa.start_loc["coordinates"]
        startb = tripb.start_loc["coordinates"]
        enda = tripa.end_loc["coordinates"]
        endb = tripb.end_loc["coordinates"]

        # Flip indices because points are in geojson (i.e. lon, lat)
        start = self.distance(starta[1], starta[0], startb[1], startb[0])
        end = self.distance(enda[1], enda[0], endb[1], endb[0])

        return True if start and end else False


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
