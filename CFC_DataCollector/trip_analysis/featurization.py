import matplotlib.pyplot as plt
import math
import numpy
from sklearn.cluster import KMeans
from kmedoid import kmedoids
from sklearn import metrics
from sklearn.metrics.cluster import homogeneity_score, completeness_score
import sys, os
sys.path.append("./base/")
from get_database import get_fake_trips_db

"""
This class is used for featurizing data, clustering the data, and evaluating the clustering. 

The input parameters of an instance of this class are:
- data (optional): Pass in a list of trips, where each entry is a dictionary containing trip_start_location and trip_end_location. Default (for now) is to load from the fake trips database. 
- ground_truth (optional): a boolean for determining if there is ground truth for this dataset. Default is false.
- colors (optional): a list of the ground truth clusters for the data, in the form of a list of integers where different integers correspond to different clusters. If ground_truth is True and colors is not None, then ground truth will be loaded from colors. If ground_truth is True and colors is None, ground truth will be collected as the 'trip_id' field of each trip. 

An example of running this class can be found in the main() function. 

"""
class featurization:

    def __init__(self, data=None, ground_truth=False, colors=None):
        if data == None:
            self.read_data()
        else:
            self.data = data
        self.calculate_points()
        if ground_truth==True:
            if colors == None:
                self.calculate_colors()
            else:
                self.colors = colors

    #load the data from the trip database. 
    def read_data(self):
        self.data = []
        db = get_fake_trips_db()
        trips = db.find()
        for t in trips:
            self.data.append(t)

    #calculate the points to use in the featurization. 
    def calculate_points(self):
        self.points = []
        for i in range(len(self.data)):
            start = self.data[i]['trip_start_location']
            end = self.data[i]['trip_end_location']
            self.points.append([start[0], start[1], end[0], end[1]])

    #calculate the ground truth, if specified. 
    def calculate_colors(self):
        col = []
        for i in range(len(self.data)):
            color = self.data[i]['label']
            col.append(color)
        self.colors = [0] * len(col)
        indices = []
        for color in col:
            if color not in indices:
                indices.append(color)
        for i in range(len(col)):
            self.colors[i] = indices.index(col[i])

    #cluster the data. input options:
    # - min_clusters (optional): the minimum number of clusters to test for. Must be at least 2. Default to 50. 
    # - max_clusters (optional): the maximum number of clusters to test for. Default to 80. 
    # - name (optional): the clustering algorithm to use. Options are 'kmeans' or 'kmedoids'. Default is kmeans.
    # - initial (optional): the way you want to initialize the means in kmeans. Options are 'random', 'k-means++', or an ndarray. Default is k-means++. 
    # - rad (optional): an option to use the custom distance metric in the kmedoid class. Default is False.
    def cluster(self, name='kmeans', min_clusters=50, max_clusters=80, initial='k-means++', rad=False):
        if min_clusters < 2:
            raise Exception('Must have at least 2 clusters to cluster the data.')
        if name != 'kmeans' and name != 'kmedoids':
            raise Exception('Invalid clustering algorithm name.')
        max = -2
        num = 0
        labely = []
        r = max_clusters - min_clusters+1

        if name == 'kmedoids':
            for i in range(r):
                num_clusters = i + min_clusters
                cl = kmedoids(self.points, num_clusters, rad)
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
                cl = KMeans(num_clusters, random_state=8, init=initial)
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

    #compute metrics to evaluate clusters
    def check_clusters(self):
        print 'number of clusters is ' + str(self.clusters)
        print 'silhouette score is ' + str(self.sil)
        print 'homogeneity is ' + str(homogeneity_score(self.colors, self.labels))
        print 'completeness is ' + str(completeness_score(self.colors, self.labels))

    #calculate the distribution of each class under the clustering
    #must have a field label in each point in data
    #provide a filename to write the data to 
    def distributions(self, filename):
        labels = [0] * len(set(self.colors))
        modes = [0] * len(set(self.colors))
        distributions = [0] * len(set(self.colors))
        names = [0] * len(set(self.colors))
        for i in range(len(labels)):
            labels[i] = []
        for i in range(len(self.colors)):
            labels[self.colors[i]].append(self.labels[i])
            names[self.colors[i]] = self.data[i]['label']
        for i in range(len(labels)):
            modes[i] = max(set(labels[i]), key=labels[i].count)
        for i in range(len(labels)):
            m = modes[i]
            count = labels[i].count(m)
            distributions[i] = float(count)/float(len(labels[i]))

        N = len(distributions)
        index = numpy.arange(N)
        width = .8
        fig, ax = plt.subplots()
        plt.bar(index+width, distributions, width, color='m')
        plt.suptitle('Percent of each cluster with same label')
        ax.set_ylim([0,1])
        plt.show()

        f = open(filename, 'w')
        distributions = dict(enumerate(distributions))
        sorteddistributions = sorted(distributions, key=lambda x: distributions[x])
        for i in range(len(set(self.colors))):
            num = sorteddistributions[i]
            n = len(set(labels[num]))
            percent = distributions[num]*100
            percent = round(percent, 2)
            percent_in_order = round(distributions[i]*100,2)
            if percent == 100.0:
                continue
            f.write('In the ' + str(names[num]) + ' cluster, ' + str(percent) + '% of the labels are the same, with ' + str(n) + ' different labels\n')
        f.close()



    #plot individual ground-truthed clusters on a map, where each map is one cluster defined 
    #by the ground truth and if two trips are the same color on a map, then they are labeled 
    #the same by the clustering algorithm
    def map_individuals(self):
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
                    start_lat = self.data[i]['trip_start_location'][1]
                    start_lon = self.data[i]['trip_start_location'][0]
                    end_lat = self.data[i]['trip_end_location'][1]
                    end_lon = self.data[i]['trip_end_location'][0]
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
        mymap = pygmaps.maps(37.5, -122.32, 10)
        for i in range(len(self.points)):
            start_lat = self.data[i]['trip_start_location'][1]
            start_lon = self.data[i]['trip_start_location'][0]
            end_lat = self.data[i]['trip_end_location'][1]
            end_lon = self.data[i]['trip_end_location'][0]
            path = [(start_lat, start_lon), (end_lat, end_lon)]
            mymap.addpath(path, matcol.rgb2hex(colormap(float(self.colors[i])/len(set(self.colors)))))
        mymap.draw('./mymap.html')
        mymap2 = pygmaps.maps(37.5, -122.32, 10)
        for i in range(len(self.points)):
            start_lat = self.data[i]['trip_start_location'][1]
            start_lon = self.data[i]['trip_start_location'][0]
            end_lat = self.data[i]['trip_end_location'][1]
            end_lon = self.data[i]['trip_end_location'][0]
            path = [(start_lat, start_lon), (end_lat, end_lon)]
            mymap2.addpath(path, matcol.rgb2hex(colormap(float(self.labels[i])/self.clusters)))
        mymap2.draw('./mylabels.html')
