from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
# standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import range
from builtins import *
from builtins import object
from past.utils import old_div
import logging
import numpy
import math
import copy
import geojson as gj

# our imports
import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.entry as ecwe
import emission.storage.decorations.analysis_timeseries_queries as esda


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

class representatives(object):

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
    #I don't understand wtf this does
    # Why are we getting the mean of the start and end points in the cluster and
    # creating a fake trip from it? Why not just pick a real representative of
    # of the trips? Alternatively, why not create a new data structure to represent
    # that this is a reconstructed trip that has no bearing in reality? What does
    # it even mean that we have a trip with only a start and end point and no
    # actual start or end times?
    def get_reps(self):
        self.reps = []
        if not self.data:
            return
        for i, cluster in enumerate(self.clusters):
            logging.debug("Considering cluster %d = %s" % (i, cluster))
            points = [[], [], [], []]

            # If this cluster has no points, we skip it
            if len(cluster) == 0:
                logging.info("Cluster %d = %s, has length %d, skipping" %
                             (i, cluster, len(cluster)))
                continue

            for j, c in enumerate(cluster):
                logging.debug("Consider point %d = %s" % (j, c))
                start_place = esda.get_entry(esda.CLEANED_PLACE_KEY,
                                             c.data.start_place)
                end_place = esda.get_entry(esda.CLEANED_PLACE_KEY,
                                             c.data.end_place)
                points[0].append(start_place.data.location["coordinates"][1]) # lat
                points[1].append(start_place.data.location["coordinates"][0]) # lng
                points[2].append(end_place.data.location["coordinates"][1]) # lat
                points[3].append(end_place.data.location["coordinates"][0]) # lng
                logging.debug("in representatives, endpoints have len = %s" %
                              len(points))
            centers = numpy.mean(points, axis=1)
            logging.debug("For cluster %d, centers are %s" % (i, centers))
            t = ecwt.Trip({
                "start_loc": gj.Point([centers[1], centers[0]]),
                "end_loc": gj.Point([centers[3], centers[2]])
            })
            a = ecwe.Entry.create_entry(c.user_id, "analysis/cleaned_trip", t)
            self.reps.append(a)

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
                    point = self.reps[b[1]].data.start_loc
                if b[0] == 'end':
                    point = self.reps[b[1]].data.end_loc
                locs.append(point.coordinates)
            locs = numpy.mean(locs, axis=0)
            coord = [locs[0], locs[1]]
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
        for i in range(self.num_clusters):
            start_places = []
            end_places = []
            for t in self.tour_dict[i]["sections"]:
                start = esda.get_object(esda.CLEANED_PLACE_KEY, t.data.start_place)
                end = esda.get_object(esda.CLEANED_PLACE_KEY, t.data.end_place)
                start_places.append(start)
                end_places.append(end)
            self.tour_dict[i]["start_places"] = start_places
            self.tour_dict[i]["end_places"] = end_places
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
            if cluster['start'] == cluster['end'] and len(self.tour_dict) > 1:
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
            pointa = self.reps[a].data.start_loc
        elif label == 'end':
            pointa = self.reps[a].data.end_loc
        for b in bin:
            if b[0] == 'start':
                pointb = self.reps[b[1]].data.start_loc
            elif b[0] == 'end':
                pointb = self.reps[b[1]].data.end_loc
            if self.distance(pointa.coordinates[1], pointa.coordinates[0],
                             pointb.coordinates[1], pointb.coordinates[0]) > 300:
                return False
        return True

    #the meter distance between two points
    def distance(self, lat1, lon1, lat2, lon2):
        R = 6371000
        rlat1 = math.radians(lat1)
        rlat2 = math.radians(lat2)
        lon = math.radians(lon2 - lon1);
        lat = math.radians(lat2-lat1);
        a = math.sin(old_div(lat,2.0))**2 + math.cos(rlat1)*math.cos(rlat2) * math.sin(old_div(lon,2.0))**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = R * c
        return d
