import emissions.analysis.modelling.cluster_pipeline as cp

"""
Functions to evaluate clustering based on groundtruth. To use these functions, 
an array of the length of the data must be passed in, with different values in the
array indicating different groundtruth clusters.
These functions can be used alongside the cluster pipeline to evaluate clustering.
An example of how to run this with the cluster pipeline is the following:

data = cp.read_data() #get the data
colors = get_colors(data, colors) #get the colors associated with the data, given a color array
data, bins = cp.remove_noise() #remove noise from the data

##### to evaluate bins, import similarity.py:
sim = similarity.similarity(data, .5, 300)
sim.bins = bins
sim.evaluate_bins()
colors = update_colors(bins, colors)
labels = sim.labels
evaluate(colors, labels)
#####

clusters, labels, data = cp.cluster(data, len(bins)) #cluster the data
evaluate(colors, labels) #evaluate the clusters
map_clusters_by_groundtruth(data, labels, colors) #map clusters

Note that the cluster pipeline works with trips, not sections, so to use the above 
code the groundtruth has to also be by trips. 
"""
#turns color array into an array of integers
def get_colors(data, colors):
    if (len(data) != len(colors)):
        except ValueError('Data and groundtruth must have the same number of elements')
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

#evaluates the cluster labels against the groundtruth colors
def evaluate(colors, labels):
    b = homogeneity_score(colors, labels)
    c = completeness_score(colors, labels)
    print 'homogeneity is ' + str(b)
    print 'completeness is ' + str(c)

#maps the clusters, colored by the groundtruth
#creates a map for each groundtruthed cluster and 
#a map showing all the clusters. 
def map_clusters_by_groundtruth(data, labels, colors):
    import pygmaps
    from matplotlib import colors as matcol
    colormap = plt.cm.get_cmap()
    import random 
    r = random.sample(range(len(set(labels))), len(set(labels)))
    rand = []
    clusters = len(set(labels))
    for i in range(len(labels)):
        rand.append(r[labels[i]]/float(clusters))
    for color in set(colors):
        first = True
        num_paths = 0
        for i in range(len(colors)):
            if colors[i] == color:
                num_paths += 1
                start_lat = data[i].trip_start_location
                start_lon = data[i].trip_start_location
                end_lat = data[i].trip_end_location
                end_lon = data[i].trip_end_location
                if first:
                    mymap = pygmaps.maps(start_lat, start_lon, 10)
                    first = False
                path = [(start_lat, start_lon), (end_lat, end_lon)]
                mymap.addpath(path, matcol.rgb2hex(colormap(rand[i])))
            mymap.draw('./mycluster' + str(color) + '.html')

    mymap = pygmaps.maps(37.5, -122.32, 10)
    for i in range(len(self.points)):
        start_lat = self.data[i].trip_start_location
        start_lon = self.data[i].trip_start_location
        end_lat = self.data[i].trip_end_location
        end_lon = self.data[i].trip_end_locatio
        path = [(start_lat, start_lon), (end_lat, end_lon)]
        mymap.addpath(path, matcol.rgb2hex(colormap(float(self.colors[i])/len(set(self.colors)))))
    mymap.draw('./mymap.html')
