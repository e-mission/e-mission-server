import emission.analysis.modelling.tour_model.tour_model_matrix as tm
import emission.core.get_database as edb
import emission.core.wrapper.trip as trip
import emission.analysis.modelling.tour_model.cluster_pipeline as eamtcp
from uuid import UUID
import random

def test():
    list_of_cluster_data = eamtcp.main()
    print list_of_cluster_data
    new_tm = create_tour_model("Josh", list_of_cluster_data)

def create_tour_model(user, list_of_cluster_data):
    # Highest level function, create tour model from the cluster data that nami gives me
    our_tm = set_up(list_of_cluster_data, user)  ## Adds nodes to graph 
    print "Got past set_up"
    print our_tm
    make_graph_edges(list_of_cluster_data, our_tm)
    print "Got past make_graph_edges"
    print our_tm
    populate_prob_field_for_locatons(list_of_cluster_data, our_tm)
    print "Got past populate_prob_field_for_locatons"
    print our_tm
    print "Got past populate_prob_field_for_modes"
    print our_tm.build_tour_model()
    return our_tm

## Second level functions that are part of main
def set_up(list_of_cluster_data, user_name):
    our_tour_model = tm.TourModel(user_name, 0, 0)
    #print list_of_cluster_data
    for dct in list_of_cluster_data:
        #print "dct is %s" % dct
        start_name = dct['start']
        end_name = dct['end']
        start_coords = cd['start_coords']
        end_coords = cd['end_coords']
        for sec in dct['sections']:
            our_tour_model.add_location(start_name, True, start_coords)
            our_tour_model.add_location(end_name, False, end_coords)
    return our_tour_model


def get_start_hour(section_info):
    return section_info.start_time.hour


def get_end_hour(section_info):
    print section_info.end_time.hour
    return section_info.end_time.hour


def get_day(section_info):
    return section_info.start_time.weekday()

def get_mode_num(section_info):
    map_modes_to_numbers = {"walking" : 0, "car" : 1, "train" : 2, "bart" : 3, "bike" : 4}
    return random.randint(0, 4)
    print section_info.sections

def make_graph_edges(list_of_cluster_data, tour_model):
    for cd in list_of_cluster_data:
        start_loc = cd['start']
        end_loc = cd['end']
        start_loc_temp = tm.Location(start_loc, tour_model)
        start_loc_temp = tour_model.get_location(start_loc_temp)
        end_loc_temp = tm.Location(end_loc, tour_model)
        end_loc_temp = tour_model.get_location(end_loc_temp)
        e = make_graph_edge(start_loc_temp, end_loc_temp, tour_model)
        for trip in cd["sections"]:
            e.add_trip(trip)

def populate_prob_field_for_locatons(list_of_cluster_data, tour_model):
    for cd in list_of_cluster_data:
        start_loc = cd['start']
        end_loc = cd['end']
        for sec in cd["sections"]:
            #print sec
            start_loc_temp = tm.Location(start_loc, tour_model)
            start_loc_temp = tour_model.get_location(start_loc_temp)
            end_loc_temp = tm.Location(end_loc, tour_model)
            end_loc_temp = tour_model.get_location(end_loc_temp)
            com = tm.Commute(start_loc_temp, end_loc_temp)
            tour_model.add_start_hour(start_loc_temp, sec.start_time)
            start_loc_temp.increment_successor(end_loc_temp, get_start_hour(sec), get_day(sec))
            #print "counter for %s is : %s" % (start_loc_temp, start_loc_temp.counter)


## Utility functions 
def get_section_obj_from_cluster_data(section_info):
    our_section = trip.Section.section_from_json(section_info)
    return our_section

def make_location_from_section(section, tour_model):
    start_hour = section.start_time.hour    

def make_graph_edge(start_point, end_point, tour_model):
    sp = tour_model.get_location(start_point)
    ep = tour_model.get_location(end_point)
    comm = tm.Commute(sp, ep)
    return tour_model.get_edge(comm)
