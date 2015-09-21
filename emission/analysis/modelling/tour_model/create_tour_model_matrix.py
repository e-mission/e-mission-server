import emission.analysis.modelling.tour_model.tour_model_matrix as tm ##here
import emission.core.get_database as edb
import emission.core.wrapper.trip as trip
import emission.analysis.modelling.tour_model.cluster_pipeline as eamtcp
import emission.simulation.trip_gen as tg
from uuid import UUID
import random, datetime, sys, argparse

def create_tour_model_from_fake_data(user_name):
    # Call with a username unique to your database
    tg.create_fake_trips(user_name)
    list_of_cluster_data = eamtcp.main(user_name)
    return create_tour_model_from_cluster_data(user_name, list_of_cluster_data)

def create_tour_model_from_cluster_data(user, list_of_cluster_data):
    # Highest level function, create tour model from the cluster data that nami gives me
    our_tm = set_up(list_of_cluster_data, user)  ## Adds nodes to graph 
    make_graph_edges(list_of_cluster_data, our_tm)
    populate_prob_field_for_locatons(list_of_cluster_data, our_tm)
    return our_tm

## Second level functions that are part of main
def set_up(list_of_cluster_data, user_name):
    time0 = datetime.datetime(1900, 1, 1, hour=0)
    our_tour_model = tm.TourModel(user_name, time0)
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
        print "making edge %s" % e
        for trip in cd["sections"]:
            e.add_trip(trip)

def populate_prob_field_for_locatons(list_of_cluster_data, tour_model):
    for cd in list_of_cluster_data:
        start_loc = cd['start']
        end_loc = cd['end']
        for sec in cd["sections"]:
            start_loc_temp = tm.Location(start_loc, tour_model)
            start_loc_temp = tour_model.get_location(start_loc_temp)
            end_loc_temp = tm.Location(end_loc, tour_model)
            end_loc_temp = tour_model.get_location(end_loc_temp)
            com = tm.Commute(start_loc_temp, end_loc_temp)
            tour_model.add_start_hour(start_loc_temp, sec.start_time)
            start_loc_temp.increment_successor(end_loc_temp, get_start_hour(sec), get_day(sec))


def generate_new_tour_model_from_tour_model(tour_model, fake_user_name, num_iterations):
    ## Create more realistic fake data, hopefully this works, which would be cool
    # so now we have a model of each day of travel, lets build a tour model for it
    # Returns a new tour model object
    time0 = datetime.datetime(1900, 1, 1, hour=0)
    new_tour_model = tm.TourModel(fake_user_name, time0)
    for _ in xrange(num_iterations):
        generate_one_iteration_for_tour_model(tour_model, new_tour_model)
    return new_tour_model

def generate_one_iteration_for_tour_model(old_tm, new_tm):
    trips = old_tm.build_tour_model(True)
    for day in trips:
        print day
        add_locations(day, new_tm)
        make_edges_from_day(day, new_tm)


def make_edges_from_day(day, tour_model):
    for loc_index in xrange(len(day) - 1):
        loc1, loc2, our_hour, our_day = day[loc_index][0], day[loc_index + 1][0], day[loc_index][1].hour, day[loc_index][1].weekday()
        comm = tm.Commute(loc1, loc2)
        key = tm.Commute.make_lookup_key(loc1, loc2)
        our_comm = tour_model.get_edge(loc1, loc2)
        our_comm.increment_prob(our_hour, our_day)

def add_locations(day, tour_model):
    if type(day) == tm.NoData:
        return
    for loc in day:
        tour_model.add_location(loc[0])


def build_tm_from_db(user_name):
    ## Returns a tour model based on a user name

    # Getting info from db
    commutes = edb.get_commute_db().find({"tm" : user_name})
    locations = edb.get_location_db().find({"tm" : user_name})
    time = edb.get_tm_db().find_one({"tm" : user_name})["time"]
    
    # Setting everything up
    coms = [ ]
    locs = [ ]
    for com in commutes:
        coms.append(tm.Commute.build_from_json(com))
    for loc in locations:
        locs.append(tm.Location.build_from_json(loc))
    for com in coms:
        com.start_point.add_to_successors(com.end_point)
    
    new_tm = tm.TourModel(user_name, time)

    # Adding locs and commutes to db
    for com in coms:
        new_tm.add_edge(com)
    for loc in locs:
        new_tm.add_location(loc)
    return new_tm

def make_tour_model_for_user(user_mame):
    list_of_cluster_data = eamtcp.main(user_name)
    return create_tour_model_from_cluster_data(user_name, list_of_cluster_data)




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
    print section_info.sections


final_tour_model = None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create a tour model')
    parser.add_argument('--fake')
    args = parser.parse_args()
    if args.fake:
        create_tour_model_from_fake_data(args.fake)
    else:
        if len(sys.argv) > 1:
            user = UUID(sys.argv[1])
        else:
            user = None
        final_tour_model = make_tour_model_for_user(user)