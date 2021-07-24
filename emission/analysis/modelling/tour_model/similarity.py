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
import pandas as pd
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
        if not data:
            self.all_data = []
        # In order to retrofit multiple invocation options without undertaking
        # a full restructuring, we will use the following structure for the
        # data
        # self.data will always represent the current state of the trips
        # self.bins will always represent the current state of the bins
        # In addition, self.all_data will represent all input trips
        # In addition, self.filtered_data will represent the output of "filter_too_short"
        # In addition, self.data_above_cutoff will represent the output of "delete_bins"
        # so even after finishing all three steps, we will have access to the
        # original input data
        # since we are only copying the lists (copy), not the objects in the
        # lists (deepcopy), this should not involve an undue user burden
        # I really don't see why we need to work with indices here, but in the absence of unit tests,
        # I am not comfortable with restructuring further
        self.all_data = data
        self.set_state(self.all_data)
        self.bins = []
        self.radius = float(radius)

    def set_state(self, in_data):
        """
        Encapsulates all the state related to this object
        so that we don't forget to update everything
        """
        self.data = copy.copy(in_data)
        self.size = len(self.data)
        

    # Pull out the trip filtration code so that we can invoke the code in
    # multiple ways (with and without filteration) depending on whether we want
    # to focus on common trips or auto-labeling
    def filter_trips(self):
        self.filtered_data = filter_too_short(self.all_data, self.radius)
        self.set_state(self.filtered_data)
        

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
                    print(f"Got exception while matching trip {a}, creating new bin")
                    added = False
            if not added:
                self.bins.append([a])
        self.bins.sort(key=lambda bin: len(bin), reverse=True)

    def calc_cutoff_bins(self):
        if len(self.bins) <= 1:
            self.newdata = self.data
            self.data_above_cutoff = self.newdata
            self.set_state(self.newdata)
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
        self.newdata = newdata if len(newdata) > 1 else copy.copy(self.data)
        self.data_above_cutoff = self.newdata
        self.set_state(self.newdata)
        self.below_cutoff = below_cutoff
        self.below_cutoff.sort(key=lambda bin: len(bin), reverse=True)

    def get_result_labels(self):
        """
        Return "labels" for the trips, to be consistent with sklearn
        implementations.  This is not otherwise compatible with sklearn, but
        would be great to submit it as an example of an absolute radius, even
        if it is computationally expensive.

        It would certainly help with:
        https://stackoverflow.com/questions/48217127/distance-based-classification
        and
        https://stackoverflow.com/questions/35971441/how-to-adjust-this-dbscan-algorithm-python

        This function sets labels based on the various states for the trips.
        Pulling this out
        as a separate function to write unit tests. This would be normally be
        trivial - we would just index the all_trip_df on the trips in each bin and
        set a unique number. However, if we have filtered before binning, then the
        trip indices in the bin are the filtered trip indices, which are different
        from all_trips indices. So we need to remap from one set of indices to
        another before we can assign the labels.
        param: all_trip_df: dataframe of all trips
        param: filtered_trip_df: dataframe of trips that were removed as "too short"
        param: bins (either all, or above cutoff only)
        
        Returns: pandas Series with labels:
        >=0 for trips in bins. the label is a unique bin id
        =-1 for long trips not in bins
        =-2 for trips that were too short
        """
        # This is a bit tricky wrt indexing, since the indices of the trips in the bin are after filtering,
        # so don't match up 1:1 with the indices in the trip dataframe
        # since we create a new dataframe for the filtered trips, they should match up with the filtered dataframe
        # but the index of the filtered dataframe is a new RangeIndex, so it doesn't work for indexing into the result series
        # so we need to follow a two-step process as below

        all_trip_df = pd.DataFrame(self.all_data)
        if hasattr(self, "filtered_data"):
            filtered_trip_df = pd.DataFrame([e for e in self.filtered_data])
            # print(filtered_trip_df)
        else:
            filtered_trip_df = None
        self.before_cutoff_bins = [[t for t in ob if t not in self.too_short_indices] for ob in self.all_bins]

        # assume that everything is noise to start with
        result_labels = pd.Series([-1] * len(all_trip_df))

        # self.bins contains the bins in the current state (either before or
        # after cutoff). Loop will not run if binning is not complete, so all
        # trips will be noise
        for i, curr_bin in enumerate(self.bins):
            if filtered_trip_df is not None:

                # get the trip ids of matching filtered trips for the current bin
                matching_filtered_trip_ids = filtered_trip_df.loc[curr_bin]._id
                # then, match by tripid to find the corresponding entries in the all_trips dataframe
                matching_all_trip_ids = all_trip_df[all_trip_df._id.isin(matching_filtered_trip_ids)].index
                # then set all those indices to the bin index
                result_labels.loc[matching_all_trip_ids] = i
            else:
                # No filteration, trip indices in the bins are from all trips
                # so we can index directly
                result_labels.loc[curr_bin] = i

        # For now, we also mark the "too short" labels with -2 to help with
        # our understanding. Consider removing this code later. This will override
        # noisy labels
        if filtered_trip_df is not None:        
            removed_trips = all_trip_df[~all_trip_df._id.isin(filtered_trip_df._id)]
            logging.debug("Detected removed trips %s" % removed_trips.index)
            result_labels.loc[removed_trips.index] = -2
        return result_labels    

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


