# Standard imports
import sys
import math
import datetime

# Our imports
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.similarity as similarity
import emission.analysis.modelling.tour_model.featurization as featurization
import emission.analysis.modelling.tour_model.representatives as representatives
from emission.core.wrapper.trip import Trip, Section
"""
This file reads the data from the section database, 
removes noise from the data, and clusters is. 

The parameters and clustering methods can be easily changed, 
but based on what works the best, the featurization and clustering 
works as follows. First, the data is read from the database. 
For featurization, each section is representated as a start point 
and an end point. Then, the section is put into bins and the lower 
half of the bins are removed. Then, the data is clustered using 
k-means. The parameter for k is currently tested in a range based 
on the number of elements, but I plan to adjust this range. 

As input, this file accepts an user's uuid from the command line. 
If no uuid is given, it will use all the trips from the trip database.

Currently, this file is defaulted not to handle ground truth, but 
for the purpose of tests, a parameter can be changed to collect ground 
truth from the database and compare the clustering to ground truth. 
To change this, in main, change ground_truth to True in the call to 
read_data. 
"""

#read the data from the database. If ground_truth is true, it will 
#take it from the 'color' field of each section in the database. 
def read_data(uuid=None, ground_truth=False):
    data = []
    db = edb.get_fake_trips_db()
    if uuid:
        trips = db.find({'user_id' : uuid})
    else:
        trips = db.find()
    for t in trips:
        trip = Trip.trip_from_json(t)
        if not trip.trip_start_location:
            print 1
        if not trip.trip_end_location:
            print 2
        if not trip.start_time:
            print 3
        data.append(trip)

    if len(data) == 0:
        raise KeyError('no trips found')

    colors = []
    if ground_truth:
        for d in data:
            colors.append(section['color'])
    if ground_truth:
        indices = [] * len(set(colors))
        for n in colors:
            if n not in indices:
                indices.append(n)
        for i in range(len(colors)):
            colors[i] = indices.index(colors[i])

    return data, colors

#put the data into bins and cut off the lower half of the bins
def remove_noise(data, cutoff, radius, colors=None):
    sim = similarity.similarity(data, cutoff, radius, colors=colors)
    sim.bin_data()
    sim.delete_bins()
    print 'number of bins: ' + str(len(sim.bins))
    return sim.data, sim.colors, len(sim.bins)

#cluster the data using k-means
def cluster(data, bins, colors=None):
    feat = featurization.featurization(data, colors=colors)
    m = len(data)
    min = bins
    max = int(math.ceil(1.33 * bins))
    feat.cluster(min_clusters=min, max_clusters=max)
    if bool(colors):
        feat.check_clusters()
    print 'number of clusters: ' + str(feat.clusters)
    return feat.clusters, feat.labels, feat.data

def cluster_to_tour_model(data, labels):
    repy = representatives.representatives(data, labels)
    repy.list_clusters()
    repy.reps()
    repy.locations()
    print 'number of locations: ' + str(repy.num_locations)
    repy.cluster_dict()

def main(uuid=None):
    data, colors = read_data(uuid, ground_truth=False)
    data, colors, bins = remove_noise(data, .5, 300, colors = colors)
    n, labels, data = cluster(data, bins, colors=colors)
    tour_dict = cluster_to_tour_model(data, labels)
    return tour_dict

if __name__=='__main__':
    main()


