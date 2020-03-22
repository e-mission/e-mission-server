from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

import emission.core.wrapper.tour_model as ecwtm
import emission.core.get_database as edb
import emission.storage.decorations.common_place_queries as esdcpq
import emission.storage.decorations.common_trip_queries as esdctq
import emission.analysis.modelling.tour_model.cluster_pipeline as eamtmcp

#################################################################################
############################ database functions #################################
#################################################################################

def save_tour_model_to_db(tour_model):
    save_places(tour_model)
    save_trips(tour_model)

def save_places(tour_model):
    for place in tour_model.common_places:
        esdcpq.save_common_place(place)

def save_trips(tour_model):
    for trip in tour_model.common_trips:
        esdctq.save_common_trip(trip)

def get_tour_model(user_id):
    common_places = get_common_places(user_id)
    common_trips = get_common_trips(user_id)
    props = {
        "user_id" : user_id,
        "common_trips" : common_trips,
        "common_places" : common_places
    }
    return ecwtm.TourModel(props)

def get_common_places(user_id):
    db = edb.get_common_place_db()
    place_jsons = db.find({"user_id" : user_id})
    return [esdcpq.make_common_place(place_json) for place_json in place_jsons]

def get_common_trips(user_id):
    db = edb.get_common_trip_db()
    trip_jsons = db.find({"user_id" : user_id})
    return [esdctq.make_new_common_trip(trip_json) for trip_json in trip_jsons]

##################################################################################

def make_tour_model_from_raw_user_data(user_id):
    try:
        list_of_cluster_data = eamtmcp.main(user_id)
        esdcpq.create_places(list_of_cluster_data, user_id)
        esdctq.set_up_trips(list_of_cluster_data, user_id)
    except ValueError as e:
       logging.debug("Got ValueError %s while creating tour model, skipping it..." % e)

