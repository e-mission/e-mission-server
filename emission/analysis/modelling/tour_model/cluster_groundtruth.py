from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
# standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import range
from builtins import *
from past.utils import old_div
import logging
from sklearn.metrics.cluster import homogeneity_score, completeness_score
import numpy 
import matplotlib.pyplot as plt

# our imports
import emission.analysis.modelling.tour_model.cluster_pipeline as cp
import emission.analysis.modelling.tour_model.similarity as similarity

"""
Functions to evaluate clustering based on groundtruth. To use these functions, 
an array of the length of the data must be passed in, with different values in the
array indicating different groundtruth clusters.
These functions can be used alongside the cluster pipeline to evaluate clustering.
An example of how to run this with the cluster pipeline is in the main method. To run it, 
pass in a list of groundtruth.
Note that the cluster pipeline works with trips, not sections, so to use the above 
code the groundtruth has to also be by trips. 
"""
#turns color array into an array of integers
def get_colors(data, colors):
    if len(data) != len(colors):
        raise ValueError('Data and groundtruth must have the same number of elements')
    indices = [] * len(set(colors))
    for n in colors:
        if n not in indices:
            indices.append(n)
    for i in range(len(colors)):
        colors[i] = indices.index(colors[i])
    return colors

#update the ground truth after binning
def update_colors(bins, colors):
    newcolors = []
    for bin in bins:
        for b in bin:
            newcolors.append(colors[b])
    indices = [] * len(set(newcolors))
    for n in newcolors:
        if n not in indices:
            indices.append(n)
    for i in range(len(newcolors)):
        newcolors[i] = indices.index(newcolors[i])
    return newcolors

#evaluates the cluster labels against the groundtruth colors
def evaluate(colors, labels):
    b = homogeneity_score(colors, labels)
    c = completeness_score(colors, labels)
    logging.debug('homogeneity is %d' % b)
    logging.debug('completeness is %d' % c)

#maps the clusters, colored by the groundtruth
#creates a map for each groundtruthed cluster and 
#a map showing all the clusters. 
def map_clusters_by_groundtruth(data, labels, colors, map_individuals=False):
    from matplotlib import colors as matcol
    colormap = plt.cm.get_cmap()
    import random 
    r = random.sample(list(range(len(set(labels)))), len(set(labels)))
    rand = []
    clusters = len(set(labels))
    for i in range(len(labels)):
        rand.append(old_div(r[labels[i]],float(clusters)))
    if map_individuals:
        for color in set(colors):
            first = True
            num_paths = 0
            for i in range(len(colors)):
                if colors[i] == color:
                    num_paths += 1
                    start_lat = data[i].trip_start_location.lat
                    start_lon = data[i].trip_start_location.lon
                    end_lat = data[i].trip_end_location.lat
                    end_lon = data[i].trip_end_location.lon
                    if first:
                        # mymap = pygmaps.maps(start_lat, start_lon, 10)
                        first = False
                    path = [(start_lat, start_lon), (end_lat, end_lon)]
                    mymap.addpath(path, matcol.rgb2hex(colormap(rand[i])))
            mymap.draw('./mycluster' + str(color) + '.html')

    # mymap = pygmaps.maps(37.5, -122.32, 10)
    for i in range(len(data)):
        start_lat = data[i].trip_start_location.lat
        start_lon = data[i].trip_start_location.lon
        end_lat = data[i].trip_end_location.lat
        end_lon = data[i].trip_end_location.lon
        path = [(start_lat, start_lon), (end_lat, end_lon)]
        mymap.addpath(path, matcol.rgb2hex(colormap(old_div(float(colors[i]),len(set(colors))))))
    mymap.draw('./mymap.html')

def main(colors):
    data = cp.read_data() #get the data
    colors = get_colors(data, colors) #make colors the right format
    data, bins = cp.remove_noise(data, .5, 300) #remove noise from data
    ###### the next few lines are to evaluate the binning
    sim = similarity.similarity(data, .5, 300) #create a similarity object
    sim.bins = bins #set the bins, since we calculated them above
    sim.evaluate_bins() #evaluate them to create the labels
    ######
    colors = update_colors(bins, colors) #update the colors to reflect deleted bins
    labels = sim.labels #get labels
    evaluate(numpy.array(colors), numpy.array(labels)) #evaluate the bins
    clusters, labels, data = cp.cluster(data, len(bins)) #cluster
    evaluate(numpy.array(colors), numpy.array(labels)) #evaluate clustering
    map_clusters_by_groundtruth(data, labels, colors, map_individuals=False) #map clusters, make last parameter true to map individual clusters
