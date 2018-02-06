from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

# Our imports
import emission.core.wrapper.trip_old as ect
import emission.core.wrapper.tripiterator as ecti
import emission.core.wrapper.filter_modules as ecfm
import emission.core.get_database as edb
import emission.analysis.modelling.user_model.query_scheduler_pipeline as eaqsp

#import Profiles

"""
High level overivew of alternative_trips_pipeline
The main method for this pipeline is calc_perturbed_trips
We first construct a trip iterator, and look at each trip where the pipelineFlags attribte "alternativesStarted" is False
For each of these trips, we create an array of trip objects, which are modifications to the original trip object with perturbed times
We also have a database collection that associates each original trip, with all of its perturbed trips
For each array of perturbed trip objects, we schedule those queries via CRON jobs
We initialize each perturbed trip as None, and we update the collection as the queries are being made
Overview of helper files relevant to this pipeline:
    -query.py -> makes a google maps query immediately to get the directions
              -> will also store the query into the database collection
    -perturb.py -> schedules calls to query.py on cron jobs
    @TODO: this doesn't exist yet, but it would make more sense than to put it in here in order to keep the code clean
    -database_util.py -> contains all the helper methods to abstract out interaction with the database
"""

# Invoked in recommendation pipeline to get perturbed trips user should consider
def calc_alternative_trips(user_trips, immediate):
    stagger = 1
    total_stagger = 0
    for existing_trip in user_trips:
        #if not existing_trip.pipelineFlags.alternativesStarted:
        existing_trip.pipelineFlags.startAlternatives()
        existing_trip.pipelineFlags.savePipelineFlags()
        if immediate:
            eaqsp.schedule_queries(existing_trip.trip_id, existing_trip.user_id, [existing_trip], immediate, total_stagger)
            total_stagger += stagger
        else:
            eaqsp.schedule_queries(existing_trip.trip_id, existing_trip.user_id, [existing_trip], immediate)

def get_alternative_for_trips(trip_it):
    # User Utility Pipeline calls this to get alternatve trips for one original trip (_id)
    alternatives = []
    tripCnt = 0
    for _trip in trip_it:
        logging.debug("Considering trip with id %s " % _trip.trip_id)
	tripCnt = tripCnt + 1
        try:
            ti = ecti.TripIterator(_trip.trip_id, ["alternatives", "get_alternatives"], ect.Alternative_Trip)
            alternatives.append(ti)
        except ecfm.AlternativesNotFound:
            alternatives.append([])
    logging.debug("tripCnt = %d, alternatives cnt = %d" % (tripCnt, len(alternatives)))
    return alternatives

def get_alternative_for_trip(trip):
    # User Utility Pipeline calls this to get alternatve trips for one original trip (_id)
    try:
        ti = ecti.TripIterator(trip.trip_id, ["alternatives", "get_alternatives"], ect.Alternative_Trip)
        return ti
    except ecfm.AlternativesNotFound:
        return []

def get_perturbed_trips(_id):
    # User Utility Pipeline calls this to get alternatve trips for one original trip (_id)
    # db = edb.get_perturbed_trips_db()
    # _id = tripObj.get_id()
    # return db.find(_id)
    return [ect.E_Mission_Trip.trip_from_json(jsonStr) for jsonStr in edb.get_perturbed_trips_db().find({'_id' : _id})].__iter__()
