# Standard imports
import math
import datetime
import uuid as uu
import sys
import logging

# Our imports
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.similarity as similarity
import emission.analysis.modelling.tour_model.featurization as featurization
import emission.analysis.modelling.tour_model.representatives as representatives
from emission.core.wrapper.trip_old import Trip, Section, Fake_Trip
import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.section as ecws
import emission.storage.decorations.trip_queries as ecsdtq
import emission.storage.decorations.section_queries as ecsdsq

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
def read_data(uuid=None, size=None, old=True):
    data = []
    trip_db = edb.get_trip_db()
    if not old:
        trip_db = edb.get_trip_new_db()
        trips = trip_db.find({"user_id" : uuid})
    else:
        if uuid:
            trips = trip_db.find({'user_id' : uuid, 'type' : 'move'})
        else:
            trips = trip_db.find({'type' : 'move'})
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
        return data
    return [ecwt.Trip(trip) for trip in trips]

#put the data into bins and cut off the lower portion of the bins
def remove_noise(data, radius, old=True):
    if not data:
        return [], []
    sim = similarity.similarity(data, radius, old)
    sim.bin_data()
    logging.debug('number of bins before filtering: %d' % len(sim.bins))
    sim.delete_bins()
    logging.debug('number of bins after filtering: %d' % len(sim.bins))
    return sim.newdata, sim.bins

#cluster the data using k-means
def cluster(data, bins, old=True):
    if not data:
        return 0, [], []
    feat = featurization.featurization(data, old=old)
    min = bins
    max = int(math.ceil(1.5 * bins))
    feat.cluster(min_clusters=min, max_clusters=max)
    logging.debug('number of clusters: %d' % feat.clusters)
    return feat.clusters, feat.labels, feat.data

#prepare the data for the tour model
def cluster_to_tour_model(data, labels, old=True):
    if not data:
        return []
    repy = representatives.representatives(data, labels, old=old)
    repy.list_clusters()
    repy.get_reps()
    repy.locations()
    logging.debug('number of locations: %d' % repy.num_locations)
    repy.cluster_dict()
    return repy.tour_dict

def main(uuid=None, old=True):
    data = read_data(uuid, old=old)
    logging.debug("len(data) is %d" % len(data))
    data, bins = remove_noise(data, 300, old=old)
    n, labels, data = cluster(data, len(bins), old=old)
    tour_dict = cluster_to_tour_model(data, labels, old=old)
    return tour_dict

if __name__=='__main__':
    uuid = None
    if len(sys.argv) == 2:
        uuid = sys.argv[1]
        uuid = uu.UUID(uuid)
    main(uuid=uuid)
