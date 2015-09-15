# Standard imports
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import math
import numpy
from sklearn.cluster import KMeans
from sklearn import metrics
import sys

# our imports
from emission.core.wrapper.trip_old import Trip, Coordinate
from kmedoid import kmedoids

"""
This class is used for featurizing data, clustering the data, and evaluating the clustering. 

The input parameters of an instance of this class are:
- data: Pass in a list of trip objects that have a trip_start_location and a trip_end_location

This class is run by cluster_pipeline.py
"""
class featurization:

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
            start = trip.trip_start_location
            end = trip.trip_end_location
            if not (start and end):
                raise AttributeError('each trip must have valid start and end locations')
            self.points.append([start.lon, start.lat, end.lon, end.lat])

    #cluster the data. input options:
    # - name (optional): the clustering algorithm to use. Options are 'kmeans' or 'kmedoids'. Default is kmeans.
    # - min_clusters (optional): the minimum number of clusters to test for. Must be at least 2. Default to 2.
    # - max_clusters (optional): the maximum number of clusters to test for. Default to the number of points. 
    def cluster(self, name='kmeans', min_clusters=2, max_clusters=None):
        if min_clusters < 2:
            min_clusters = 2
        if min_clusters > len(self.points):
            sys.stderr.write('Maximum number of clusters is the number of data points.\n')
            min_clusters = len(self.points)-1
        if max_clusters == None:
            max_clusters = len(self.points)-1
        if max_clusters < 2:
            sys.stderr.write('Must have at least 2 clusters\n')
            max_clusters = 2
        if max_clusters >= len(self.points):
            max_clusters = len(self.points)-1
        if max_clusters < min_clusters:
            raise ValueError('Please provide a valid range of cluster sizes')
        if name != 'kmeans' and name != 'kmedoids':
            print 'Invalid clustering algorithm name. Defaulting to k-means'
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
                print 'testing ' + str(num_clusters) + ' clusters'
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
            print 'Please cluster before analyzing clusters.'
            return
        print 'number of clusters is ' + str(self.clusters)
        print 'silhouette score is ' + str(self.sil) 

    #map the clusters
    #TODO - move this to a file in emission.analysis.plotting to map clusters from the database
    def map_clusters(self):
        import pygmaps
        from matplotlib import colors as matcol
        colormap = plt.cm.get_cmap()

        if self.labels:
            mymap2 = pygmaps.maps(37.5, -122.32, 10)
            for i in range(len(self.points)):
                start_lat = self.points[i][1]
                start_lon = self.points[i][0]
                end_lat = self.points[i][3]
                end_lon = self.points[i][2]
                path = [(start_lat, start_lon), (end_lat, end_lon)]
                mymap2.addpath(path, matcol.rgb2hex(colormap(float(self.labels[i])/self.clusters)))
            mymap2.draw('./mylabels.html')
