from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import range
from builtins import *
from builtins import object
from past.utils import old_div
import logging
import matplotlib.pyplot as plt
import numpy
from sklearn.cluster import KMeans
from sklearn import metrics
import sys

# our imports
from .kmedoid import kmedoids


"""
This class is used for featurizing data, clustering the data, and evaluating the clustering. 

The input parameters of an instance of this class are:
- data: Pass in a list of trip objects that have a trip_start_location and a trip_end_location

This class is run by cluster_pipeline.py
"""
class featurization(object):

    def __init__(self, data):
        self.data = data
        if not self.data:
            self.data = []
        self.calculate_points()
        self.labels = []
        self.clusters = None


    #calculate the points to use in the featurization. 
    def calculate_points(self):
        self.points = []
        if not self.data:
            return
        for trip in self.data:
            try:
                start = trip.data.start_loc["coordinates"]
                end = trip.data.end_loc["coordinates"]
            except:
                continue
            if not (start and end):
                raise AttributeError('each trip must have valid start and end locations')
            self.points.append([start[0], start[1], end[0], end[1]])

    #cluster the data. input options:
    # - name (optional): the clustering algorithm to use. Options are 'kmeans' or 'kmedoids'. Default is kmeans.
    # - min_clusters (optional): the minimum number of clusters to test for. Must be at least 2. Default to 2.
    # - max_clusters (optional): the maximum number of clusters to test for. Default to the number of points. 
    def cluster(self, name='kmeans', min_clusters=2, max_clusters=None):
        logging.debug("min_clusters = %s, max_clusters = %s, len(self.points) = %s" % 
            (min_clusters, max_clusters, len(self.points)))
        if min_clusters < 2:
            logging.debug("min_clusters < 2, setting min_clusters = 2")
            min_clusters = 2
        if min_clusters > len(self.points):
            sys.stderr.write('Minimum number of clusters %d is greater than the number of data points %d.\n' % (min_clusters, len(self.points)))
            min_clusters = len(self.points)-1
        if max_clusters == None:
            logging.debug("max_clusters is None, setting max_clusters = %d" % (len(self.points) - 1))
            max_clusters = len(self.points)-1
        if max_clusters < 2:
            sys.stderr.write('Must have at least 2 clusters\n')
            max_clusters = 2
        if max_clusters >= len(self.points):
            logging.debug("max_clusters >= len(self.points), setting max_clusters = %d" % (len(self.points) - 1))
            max_clusters = len(self.points)-1
        if max_clusters < min_clusters:
            raise ValueError('Please provide a valid range of cluster sizes')
        if name != 'kmeans' and name != 'kmedoids':
            logging.debug('Invalid clustering algorithm name. Defaulting to k-means')
            name='kmeans'
        if not self.data:
            self.sil = None
            self.clusters = 0
            self.labels = []
            return []
        max = -2
        num = 0
        labely = []
        r = max_clusters - min_clusters+1
        if name == 'kmedoids':
            for i in range(r):
                num_clusters = i + min_clusters
                logging.debug('testing %s clusters' % str(num_clusters))
                cl = kmedoids(self.points, num_clusters)
                self.labels = [0] * len(self.data)
                cluster = -1
                for key in cl[2]:
                    cluster += 1
                    for j in cl[2][key]:
                        self.labels[j] = cluster
                sil = metrics.silhouette_score(numpy.array(self.points), numpy.array(self.labels))
                if sil > max:
                    max = sil
                    num = num_clusters
                    labely = self.labels
        elif name == 'kmeans':
            import warnings
            for i in range(r):
                num_clusters = i + min_clusters
                if num_clusters == 0:
                   continue
                cl = KMeans(num_clusters, random_state=8)
                cl.fit(self.points)
                self.labels = cl.labels_
                warnings.filterwarnings("ignore")                
                sil = metrics.silhouette_score(numpy.array(self.points), self.labels)
                if sil > max:
                    max = sil
                    num = num_clusters
                    labely = self.labels
        self.sil = max
        self.clusters = num
        self.labels = labely
        self.labels = list(self.labels)
        return self.labels

    #compute metrics to evaluate clusters
    def check_clusters(self):
        if not self.clusters:
            sys.stderr.write('No clusters to analyze\n')
            return
        if not self.labels:
            logging.debug('Please cluster before analyzing clusters.')
            return
        logging.debug('number of clusters is %d' % self.clusters)
        logging.debug('silhouette score is %s' % self.sil)

    #map the clusters
    #TODO - move this to a file in emission.analysis.plotting to map clusters from the database
    def map_clusters(self):
        from matplotlib import colors as matcol
        colormap = plt.cm.get_cmap()

        if self.labels:
            # mymap2 = pygmaps.maps(37.5, -122.32, 10)
            for i in range(len(self.points)):
                start_lat = self.points[i][1]
                start_lon = self.points[i][0]
                end_lat = self.points[i][3]
                end_lon = self.points[i][2]
                path = [(start_lat, start_lon), (end_lat, end_lon)]
                mymap2.addpath(path, matcol.rgb2hex(colormap(old_div(float(self.labels[i]),self.clusters))))
            mymap2.draw('./mylabels.html')
