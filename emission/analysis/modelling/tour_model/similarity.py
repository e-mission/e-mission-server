from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import range
from builtins import *
from builtins import object
from past.utils import old_div
import logging
import math
import numpy
import copy
from sklearn import metrics
from numpy.linalg import norm
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.core.common as ecc


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

#Determine whether two points are within a cutoff distance threshold
def within_radius(lat1, lon1, lat2, lon2, radius):
    dist = ecc.calDistance([lon1, lat1], [lon2, lat2])
    return dist <= radius

def filter_too_short(all_trips, radius):
    # Since this is a pure function, we should append valid trips
    # instead of removing invalid trips
    # But let's go from working to working
    valid_trips = copy.copy(all_trips)
    for t in all_trips:
        logging.debug(f"Considering trip {t['_id']}: {t.data.start_fmt_time} -> {t.data.end_fmt_time}, {t.data.start_loc} -> {t.data.end_loc}")
        try:
            start_lon = t.data.start_loc["coordinates"][0]
            start_lat = t.data.start_loc["coordinates"][1]
            end_lon = t.data.end_loc["coordinates"][0]
            end_lat = t.data.end_loc["coordinates"][1]
            logging.debug("endpoints are = (%s, %s) and (%s, %s)" %
                          (start_lon, start_lat, end_lon, end_lat))
            if within_radius(start_lat, start_lon, end_lat, end_lon, radius):
                valid_trips.remove(t)
        except:
            logging.exception("exception while getting start and end places for %s" % t)
            valid_trips.remove(t)

    logging.debug('After removing trips that are points, there are %s data points' % len(valid_trips))
    return valid_trips

class similarity(object):
    def __init__(self, data, radius):
        inData = data
        if not data:
            inData = []
        self.bins = []
        self.radius = float(radius)
        self.data = filter_too_short(inData, self.radius)
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

    def calc_cutoff_bins(self):
        if len(self.bins) <= 1:
            self.newdata = self.data
            return
        num = self.elbow_distance()
        logging.debug("bins = %s, elbow distance = %s" % (self.bins, num))
        sum = 0
        for i in range(len(self.bins)):
            sum += len(self.bins[i])
            if len(self.bins[i]) <= len(self.bins[num]):
                logging.debug("found weird condition, self.bins[i] = %s, self.bins[num] = %s" %
                    (self.bins[i], self.bins[num]))
                sum -= len(self.bins[i])
                num = i
                break
        logging.debug('the new number of trips is %d' % sum)
        logging.debug('the cutoff point is %d' % num)
        self.num = num

    #delete lower portion of bins
    def delete_bins(self):
        below_cutoff =[]
        self.calc_cutoff_bins()
        for i in range(len(self.bins) - self.num):
            below_cutoff.append(self.bins.pop())
        newdata = []
        for bin in self.bins:
            for b in bin:
                d = self.data[b]
                newdata.append(self.data[b])
        self.newdata = newdata if len(newdata) > 1 else self.data
        self.below_cutoff = below_cutoff
        self.below_cutoff.sort(key=lambda bin: len(bin), reverse=True)



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
        x = list(range(N))
        max = 0
        index = -1
        a = numpy.array([x[0], y[0]])
        b = numpy.array([x[-1], y[-1]])
        n = norm(b-a)
        new_y = []
        for i in range(0, N):
            p = numpy.array([x[i], y[i]])
            dist = old_div(norm(numpy.cross(p-a,p-b)),n)
            new_y.append(dist)
            if dist > max:
                max = dist
                index = i
        return index

    #check if two trips match
    def match(self,a,bin):
        for b in bin:
            if not self.distance_helper(a, b):
                return False
        return True

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
                tb = self.data[b]
                start_lon = tb.data.start_loc["coordinates"][0]
                start_lat = tb.data.start_loc["coordinates"][1]
                end_lon = tb.data.end_loc["coordinates"][0]
                end_lat = tb.data.end_loc["coordinates"][1]
                path = [start_lat, start_lon, end_lat, end_lon]
                points.append(path)
        logging.debug("number of labels are %d, number of points are = %d" %
                      (len(labels), len(points)))
        a = metrics.silhouette_score(numpy.array(points), labels)
        logging.debug('number of bins is %d' % len(self.bins))
        logging.debug('silhouette score is %d' % a)
        return a

    #calculate the distance between two trips
    def distance_helper(self, a, b):
        tripa = self.data[a].data
        tripb = self.data[b].data

        starta = tripa.start_loc["coordinates"]
        startb = tripb.start_loc["coordinates"]
        enda = tripa.end_loc["coordinates"]
        endb = tripb.end_loc["coordinates"]

        # Flip indices because points are in geojson (i.e. lon, lat)
        start = within_radius(starta[1], starta[0], startb[1], startb[0], self.radius)
        end = within_radius(enda[1], enda[0], endb[1], endb[0], self.radius)

        return True if start and end else False


