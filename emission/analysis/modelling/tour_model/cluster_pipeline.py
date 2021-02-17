from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import math
import uuid as uu
import sys
import logging

# Our imports
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.similarity as similarity
import emission.analysis.modelling.tour_model.featurization as featurization
import emission.analysis.modelling.tour_model.representatives as representatives
import emission.storage.decorations.analysis_timeseries_queries as esda

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
def read_data(uuid=None):
    trips = esda.get_entries(esda.CLEANED_TRIP_KEY, uuid,
                             time_query=None, geo_query=None)
    logging.info("After reading data, returning %s trips" % len(trips))
    return trips

#put the data into bins and cut off the lower portion of the bins
def remove_noise(data, radius):
    if not data:
        return [], []
    sim = similarity.similarity(data, radius)
    sim.bin_data()
    logging.debug('number of bins before filtering: %d' % len(sim.bins))
    sim.delete_bins()
    logging.debug('number of bins after filtering: %d' % len(sim.bins))
    return sim.newdata, sim.bins

#cluster the data using k-means
def cluster(data, nBins):
    logging.debug("Calling cluster(%s, %d)" % (data, nBins))
    if not data:
        return 0, [], []
    feat = featurization.featurization(data)
    min = nBins
    max = int(math.ceil(1.5 * nBins))
    feat.cluster(min_clusters=min, max_clusters=max)
    logging.debug('number of clusters: %d' % feat.clusters)
    return feat.clusters, feat.labels, feat.data, feat.points

#prepare the data for the tour model
def cluster_to_tour_model(data, labels):
    if not data:
        return []
    repy = representatives.representatives(data, labels)
    repy.list_clusters()
    repy.get_reps()
    repy.locations()
    logging.debug('number of locations: %d' % repy.num_locations)
    repy.cluster_dict()
    return repy.tour_dict

def main(uuid=None):
    data = read_data(uuid)
    logging.debug("len(data) is %d" % len(data))
    data, bins = remove_noise(data, 300)
    n, labels, data = cluster(data, len(bins))
    tour_dict = cluster_to_tour_model(data, labels)
    return tour_dict

if __name__=='__main__':
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
        level=logging.DEBUG)
    uuid = None
    if len(sys.argv) == 2:
        uuid = sys.argv[1]
        uuid = uu.UUID(uuid)
    main(uuid=uuid)
