from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

import emission.analysis.modelling.tour_model.tour_model_matrix as tm
import emission.analysis.modelling.tour_model.cluster_pipeline as eamtcp
from uuid import UUID
import random, datetime, sys

def create_tour_model(user, list_of_cluster_data):
    # Highest level function, create tour model from the cluster data that nami gives me
    our_tm = set_up(list_of_cluster_data, user)  ## Adds nodes to graph 
    make_graph_edges(list_of_cluster_data, our_tm)
    populate_prob_field_for_locatons(list_of_cluster_data, our_tm)
    return our_tm

## Second level functions that are part of main
def set_up(list_of_cluster_data, user_name):
    time0 = datetime.datetime(1900, 1, 1, hour=0)
    our_tour_model = tm.TourModel(user_name, 0, time0)
    for dct in list_of_cluster_data:
        start_name = dct['start']
        end_name = dct['end']
        start_coords = dct['start_coords']
        end_coords = dct['end_coords']
        for sec in dct['sections']:
            start_loc = tm.Location(start_name, our_tour_model)
            end_loc = tm.Location(end_name, our_tour_model)
            our_tour_model.add_location(start_loc, start_coords)
            our_tour_model.add_location(end_loc, end_coords)
    return our_tour_model


def make_graph_edges(list_of_cluster_data, tour_model):
    for cd in list_of_cluster_data:
        start_loc = cd['start']
        end_loc = cd['end']
        start_loc_temp = tm.Location(start_loc, tour_model)
        start_loc_temp = tour_model.get_location(start_loc_temp)
        end_loc_temp = tm.Location(end_loc, tour_model)
        end_loc_temp = tour_model.get_location(end_loc_temp)
        e = make_graph_edge(start_loc_temp, end_loc_temp, tour_model)
        logging.debug("making edge %s" % e)
        for trip_entry in cd["sections"]:
            e.add_trip(trip_entry)

def populate_prob_field_for_locatons(list_of_cluster_data, tour_model):
    for cd in list_of_cluster_data:
        start_loc = cd['start']
        end_loc = cd['end']
        for model_trip in cd["sections"]:
            start_loc_temp = tm.Location(start_loc, tour_model)
            start_loc_temp = tour_model.get_location(start_loc_temp)
            end_loc_temp = tm.Location(end_loc, tour_model)
            end_loc_temp = tour_model.get_location(end_loc_temp)
            com = tm.Commute(start_loc_temp, end_loc_temp)
            tour_model.add_start_hour(start_loc_temp, model_trip.start_time)
            start_loc_temp.increment_successor(end_loc_temp,
                                               get_start_hour(model_trip),
                                               get_day(model_trip))


## Utility functions
def make_graph_edge(start_point, end_point, tour_model):
    sp = tour_model.get_location(start_point)
    ep = tour_model.get_location(end_point)
    comm = tm.Commute(sp, ep)
    tour_model.add_edge(comm)
    return comm

def get_start_hour(section_info):
    return section_info.start_time.hour

def get_end_hour(section_info):
    return section_info.start_time.hour

def get_day(section_info):
    return section_info.start_time.weekday()

def get_mode_num(section_info):
    map_modes_to_numbers = {"walking" : 0, "car" : 1, "train" : 2, "bart" : 3, "bike" : 4}
    return random.randint(0, 4)


final_tour_model = None

if __name__ == "__main__":
    if len(sys.argv) > 1:
        user = UUID(sys.argv[1])
    else:
        user = None
    list_of_cluster_data = eamtcp.main(user)
    final_tour_model = create_tour_model("shankari", list_of_cluster_data)
