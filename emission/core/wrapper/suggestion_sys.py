import emission.storage.timeseries.abstract_timeseries as esta
import pandas as pd
from uuid import UUID

"""
def calculate_suggestions():
    #For each person, create their most recent suggestion
    user_carbon_map = {}
    all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"uuid": 1, "_id": 0})))
    #unfinished, not sure if right direction yet
    for index, row in all_users.iterrows():
        user_id = row['uuid']
        user_carbon_map[user_id] = self.computeCarbon(user_id, TierSys.getLatest()['created_at'])
"""

def calculate_single_suggestion(uuid):
    #Given a single UUID, create a suggestion for them
    return_obj = {message: "Good job walking and biking! No suggestion to show.",
    savings: "You could save: 0 kg CO2", start_lat = 0.0, start_lon = 0.0,
    end_lat = 0.0, end_lon = 0.0}
    all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"uuid": 1, "_id": 0})))
    user_id = row['uuid']
    time_series = esta.TimeSeries.get_time_series(user_id)
    cleaned_sections = time_series.get_data_df("analysis/cleaned_section", time_query = None)
    #Go in reverse order because we check by most recent trip
    for i in range(len(cleaned_sections), -1, -1):
        distance_in_miles = cleaned_sections.iloc[i]["distance"] * 0.000621371
        mode = cleaned-sections.iloc[i]["sensed_mode"]
        if mode == 0 and distance => 5 and distance <= 15:
            #Suggest bus if it is car and distance between 5 and 15
            #TODO: Change ret_obj and figure out how to change lat and lon to places
            break
        #TODO: Make mode correspond to bus too
        elif mode == 0 and distance < 5:
            #Suggest bike if it is car/bus and distance less than 5
            #TODO: Change ret_boj and figure out how to change lat and lon to places
            break
    return return_obj
