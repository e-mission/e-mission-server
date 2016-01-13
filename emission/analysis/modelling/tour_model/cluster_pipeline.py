# Standard imports
import math
import datetime
import uuid as uu
import sys

# Our imports
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.similarity as similarity
import emission.analysis.modelling.tour_model.featurization as featurization
import emission.analysis.modelling.tour_model.representatives as representatives
from emission.core.wrapper.trip import Trip, Section, Fake_Trip
"""
This file reads the data from the trip database, 
removes noise from the data, clusters it, and returns a dictionary 
to make the tour model. 

The parameters and clustering methods can be easily changed, 
but based on what works the best, the featurization and clustering 
works as follows. First, the data is read from the database. 
For featurization, each trip is representated as a start point 
and an end point. Then, the trips are put into bins and the lower 
half of the bins are removed. Then, the data is clustered using 
k-means. The parameter for k is currently tested in a range based 
on the number of bins. This parameter may change.

As input, this file can accepts an user's uuid from the command line. 
If no uuid is given, it will use all the trips from the trip database.

It also accepts a size parameter, which will limit the number of trips 
read from the database. 
"""

#read the data from the database. 
def read_data(uuid=None,size=None):
    data = []
    db = edb.get_trip_db()
    if uuid:
        trips = db.find({'user_id' : uuid, 'type' : 'move'})
    else:
        trips = db.find({'type' : 'move'})
    if trips.count() == 0: 
        print "no trips"
        return [] 
    for t in trips:
        try: 
            trip = Trip.trip_from_json(t)
        except:
            continue
        if not (trip.trip_start_location and trip.trip_end_location and trip.start_time):
            continue
        data.append(trip)
        if size:
            if len(data) == size:
                break
    if len(data) == 0: 
        print "no sections"
        return [] 
    return data

#put the data into bins and cut off the lower portion of the bins
def remove_noise(data, radius):
    if not data:
        return [], []
    sim = similarity.similarity(data, radius)
    sim.bin_data()
    print 'number of bins before filtering: ' + str(len(sim.bins))
    sim.delete_bins()
    print 'number of bins after filtering: ' + str(len(sim.bins))
    return sim.newdata, sim.bins

#cluster the data using k-means
def cluster(data, bins):
    if not data:
        return 0, [], []
    feat = featurization.featurization(data)
    min = bins
    max = int(math.ceil(1.5 * bins))
    feat.cluster(min_clusters=min, max_clusters=max)
    print 'number of clusters: ' + str(feat.clusters)
    return feat.clusters, feat.labels, feat.data

#prepare the data for the tour model
def cluster_to_tour_model(data, labels):
    if not data:
        return []
    repy = representatives.representatives(data, labels)
    repy.list_clusters()
    repy.get_reps()
    repy.locations()
    print 'number of locations: ' + str(repy.num_locations)
    repy.cluster_dict()
    return repy.tour_dict

def main(uuid=None):
    data = read_data(uuid)
    print len(data)
    data, bins = remove_noise(data, 300)
    n, labels, data = cluster(data, len(bins))
    tour_dict = cluster_to_tour_model(data, labels)
    return tour_dict

if __name__=='__main__':
    uuid = None
    if len(sys.argv) == 2:
        uuid = sys.argv[1]
        uuid = uu.UUID(uuid)
    main(uuid=uuid)