# Standard imports
import math

# Our imports
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.similarity as similarity
import emission.analysis.modelling.tour_model.featurization as featurization
import emission.analysis.modelling.tour_model.representatives as representatives
from emission.core.wrapper.trip import Trip
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
"""

#read the data from the database. If ground_truth is true, it will 
#take it from the 'color' field of each section in the database. 
def read_data(uuid=None):
    data = []
    db = edb.get_trip_db()
    if uuid:
        trips = db.find({'user_id' : uuid})
    else:
        trips = db.find()
    for t in trips:
        trip = Trip.trip_from_json(t)
        if not (trip.trip_start_location and trip.trip_end_location and trip.start_time):
            continue
        data.append(trip)
    if len(data) == 0:
        raise KeyError('no trips found')
    return data

#put the data into bins and cut off the lower half of the bins
def remove_noise(data, cutoff, radius):
    sim = similarity.similarity(data, cutoff, radius)
    sim.bin_data()
    sim.delete_bins()
    print 'number of bins: ' + str(len(sim.bins))
    return sim.newdata, sim.bins

#cluster the data using k-means
def cluster(data, bins):
    feat = featurization.featurization(data)
    min = bins
    max = int(math.ceil(1.33 * bins))
    feat.cluster(min_clusters=min, max_clusters=max)
    print 'number of clusters: ' + str(feat.clusters)
    return feat.clusters, feat.labels, feat.data

def cluster_to_tour_model(data, labels):
    repy = representatives.representatives(data, labels)
    repy.list_clusters()
    repy.get_reps()
    repy.locations()
    print 'number of locations: ' + str(repy.num_locations)
    repy.cluster_dict()
    return repy.tour_dict

def main(uuid=None):
    data = read_data(uuid)
    data, bins = remove_noise(data, .5, 300)
    n, labels, data = cluster(data, len(bins))
    tour_dict = cluster_to_tour_model(data, labels)
    return tour_dict

if __name__=='__main__':
    main()


