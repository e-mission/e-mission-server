# Standard imports
import matplotlib.pyplot as plt
import math
import numpy
from sklearn.cluster import KMeans
from kmedoid import kmedoids
from sklearn import metrics
from sklearn.metrics.cluster import homogeneity_score, completeness_score
import sys, os

"""
This class is used for featurizing data, clustering the data, and evaluating the clustering. 

The input parameters of an instance of this class are:
- data: Pass in a list of trips in the format of the database. 
- colors (optional): a list of the ground truth clusters for the data, in the form of a list of integers where different integers correspond to different clusters. If there is no ground truth, colors defaults to None. 

This class is run by cluster_pipeline.py
"""
class featurization:

    def __init__(self, data, colors=None):
        self.data = data
        self.calculate_points()
        self.colors = colors
        self.labels = []

    #calculate the points to use in the featurization. 
    def calculate_points(self):
        if not self.data:
            raise ValueError('No data')
        self.points = []
        for i in range(len(self.data)):
            start = self.data[i]['section_start_point']['coordinates']
            end = self.data[i]['section_end_point']['coordinates']
            self.points.append([start[0], start[1], end[0], end[1]])


    #cluster the data. input options:
    # - min_clusters (optional): the minimum number of clusters to test for. Must be at least 2. Default to 2.
    # - max_clusters (optional): the maximum number of clusters to test for. Default to the number of points. 
    # - name (optional): the clustering algorithm to use. Options are 'kmeans' or 'kmedoids'. Default is kmeans.
    def cluster(self, name='kmeans', min_clusters=2, max_clusters=None):
        if min_clusters < 2:
            min_clusters = 2
        if min_clusters > len(self.points):
            sys.stderr.write('Maximum number of clusters is the number of data points.\n')
            min_clusters = len(self.points)
        if max_clusters == None:
            max_clusters = len(self.points)
        if max_clusters < 2:
            sys.stderr.write('Must have at least 2 clusters\n')
            max_clusters = 2
        if max_clusters > len(self.points):
            max_clusters = len(self.points)
        if max_clusters < min_clusters:
            sys.stderr.write('Please provide a valid range of cluster sizes\n')
            return
        if name != 'kmeans' and name != 'kmedoids':
            print 'Invalid clustering algorithm name. Defaulting to k-means'
            name='kmeans'
        if not self.points:
            raise ValueError("No data to cluster")
            return
        max = -2
        num = 0
        labely = []
        r = max_clusters - min_clusters+1
        if name == 'kmedoids':
            for i in range(r):
                num_clusters = i + min_clusters
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
            for i in range(r):
                num_clusters = i + min_clusters
                cl = KMeans(num_clusters, random_state=8)
                cl.fit(self.points)
                self.labels = cl.labels_
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
        if not self.colors:
            sys.stderr.write('No ground truth, nothing to analyze\n')
            return
        if not self.clusters:
            sys.stderr.write('No clusters to analyze\n')
            return
        if not self.labels:
            print 'Please cluster before analyzing clusters.'
            return
        print 'number of clusters is ' + str(self.clusters)
        print 'silhouette score is ' + str(self.sil)
        print 'homogeneity is ' + str(homogeneity_score(self.colors, self.labels))
        print 'completeness is ' + str(completeness_score(self.colors, self.labels))

    #plot individual ground-truthed clusters on a map, where each map is one cluster defined 
    #by the ground truth and if two trips are the same color on a map, then they are labeled 
    #the same by the clustering algorithm
    def map_individuals(self):
        if not self.labels:
            print 'Please cluster before mapping clusters.'
            return
        if not self.colors:
            print 'This function is for use with ground truthed data.'
            return
        import pygmaps
        from matplotlib import colors as matcol
        colormap = plt.cm.get_cmap()
        import random 
        r = random.sample(range(len(set(self.labels))), len(set(self.labels)))
        rand = []
        for i in range(len(self.labels)):
            rand.append(r[self.labels[i]]/float(self.clusters))
        for color in set(self.colors):
            first = True
            num_paths = 0
            for i in range(len(self.colors)):
                if self.colors[i] == color:
                    num_paths += 1
                    start_lat = self.data[i]['section_start_point']['coordinates'][1]
                    start_lon = self.data[i]['section_start_point']['coordinates'][0]
                    end_lat = self.data[i]['section_end_point']['coordinates'][1]
                    end_lon = self.data[i]['section_end_point']['coordinates'][0]
                    if first:
                        mymap = pygmaps.maps(start_lat, start_lon, 10)
                        first = False
                    path = [(start_lat, start_lon), (end_lat, end_lon)]
                    mymap.addpath(path, matcol.rgb2hex(colormap(rand[i])))
            if num_paths > 1:
                mymap.draw('./mycluster' + str(color) + '.html')
            else:
                mymap.draw('./onemycluster' + str(color) + '.html') #clusters with only one trip

    #plot all the clusters on the map. Outputs mymap.html, a map with colors defined by the ground 
    #truth, and mylabels.html, a map with colors defined by the clustering algorithm. 
    def map_clusters(self):
        import pygmaps
        from matplotlib import colors as matcol
        colormap = plt.cm.get_cmap()

        if self.colors:
            mymap = pygmaps.maps(37.5, -122.32, 10)
            for i in range(len(self.points)):
                start_lat = self.data[i]['section_start_point']['coordinates'][1]
                start_lon = self.data[i]['section_start_point']['coordinates'][0]
                end_lat = self.data[i]['section_end_point']['coordinates'][1]
                end_lon = self.data[i]['section_end_point']['coordinates'][0]
                path = [(start_lat, start_lon), (end_lat, end_lon)]
                mymap.addpath(path, matcol.rgb2hex(colormap(float(self.colors[i])/len(set(self.colors)))))
            mymap.draw('./mymap.html')
        if self.labels:
            mymap2 = pygmaps.maps(37.5, -122.32, 10)
            for i in range(len(self.points)):
                start_lat = self.data[i]['section_start_point']['coordinates'][1]
                start_lon = self.data[i]['section_start_point']['coordinates'][0]
                end_lat = self.data[i]['section_end_point']['coordinates'][1]
                end_lon = self.data[i]['section_end_point']['coordinates'][0]
                path = [(start_lat, start_lon), (end_lat, end_lon)]
                mymap2.addpath(path, matcol.rgb2hex(colormap(float(self.labels[i])/self.clusters)))
            mymap2.draw('./mylabels.html')


