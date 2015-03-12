from trips import E_Mission_Trip

#main function, converting json into a list of E_Mission_Trips
def get_trips_from_json_section():
    return [E_Mission_Trip("Dummy Trip", None, [], 0, 1, (0,0), (0,0))]

#Queries database, returns list of all trips for user
def get_sections_for_user(user_ID):
    return {}

#Removes place data, only leaving actual movement (leg) data
def filter_places():
    return 
     

