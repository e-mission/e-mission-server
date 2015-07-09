import matplotlib.pyplot as plt
import math
import numpy
from sklearn.cluster import KMeans
#from base.get_database import get_fake_trips_db
from kmedoid import kmedoids
from sklearn import metrics
from sklearn.cluster import DBSCAN
from sklearn.metrics.cluster import homogeneity_score, completeness_score
import sys, os
sys.path.append("./base/")
from get_database import get_fake_trips_db

class featurization:

    def __init__(self, data=None, colors=None, min=50, max=80, name='kmeans'):
        if data == None:
            self.read_data()
        else:
            self.data = data
        self.calculate_points()
        if colors == None:
            self.calculate_colors()
        else:
            self.colors = colors
        self.cluster(name, min, max)
        #self.map_clusters()
        self.check_clusters()

    def read_data(self):
        self.data = []
        db = get_fake_trips_db()
        trips = db.find()
        for t in trips:
            self.data.append(t)

    def calculate_points(self):
        self.points = []
        for i in range(len(self.data)):
            start = self.data[i]['trip_start_location']
            end = self.data[i]['trip_end_location']
            self.points.append([start[0], start[1], end[0], end[1]])

    def calculate_colors(self):
        col = []
        locations = set()
        for i in range(len(self.data)):
            color = self.data[i]['trip_id']
            col.append(color)
            index = color.index(' to ')
            locations.add(color[:index])
            locations.add(color[index+4:])
        self.colors = [0] * len(col)
        indices = []
        for color in col:
            if color not in indices:
                indices.append(color)
        for i in range(len(col)):
            self.colors[i] = indices.index(col[i])

    def cluster(self, name, min_clusters, max_clusters):
        max = 0
        num = 0
        labely = []
        r = max_clusters - min_clusters+1
        if name == 'kmedoids' or name == 'kmeans':
            for i in range(r):
                num_clusters = i + min_clusters
                if name == 'kmedoids':
                    cl = kmedoids(self.data, num_clusters)
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
                    cl = KMeans(num_clusters, random_state=8)
                    cl.fit(self.points)
                    self.labels = cl.labels_
                    print num_clusters
                    sil = metrics.silhouette_score(numpy.array(self.points), self.labels)
                    if sil > max:
                        max = sil
                        num = num_clusters
                        labely = self.labels
        elif name == 'dbscan':
            cl = DBSCAN()
            cl.fit(self.points)
            labely = cl.labels_
            num = len(set(labely))
        self.clusters = num
        print self.clusters
        self.labels = labely
        print self.labels

    def check_clusters(self):
        """
        num = 0
        for i in range(len(set(self.colors))):
            a = self.colors.count(i)
            b = list(self.labels).count(self.labels[i+num])
            num += a
            print str(a) + ', ' + str(b)
        """
        print 'homogeneity is ' + str(homogeneity_score(self.colors, self.labels))
        print 'completeness is ' + str(completeness_score(self.colors, self.labels))

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
            mymap.addpath(path, matcol.rgb2hex(colormap(float(self.colors[i])/self.clusters)))
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

if __name__ == "__main__":
    clusteries = featurization()

