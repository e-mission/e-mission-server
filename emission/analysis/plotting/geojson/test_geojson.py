import emission.core.get_database as edb
import pandas as pd
from uuid import UUID
import emission.analysis.plotting.geojson.geojson_feature_converter as gfc
import emission.analysis.plotting.leaflet_osm.our_plotter as lo
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.core.wrapper.entry as ecwe
import emission.storage.decorations.trip_queries as esdt
import emission.storage.timeseries.timequery as estt

all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"user_email":1, "uuid": 1, "_id": 0})))
test_user_id = all_users.iloc[60].uuid

ts = esta.TimeSeries.get_time_series(test_user_id)

# Get all cleaned trips for the first user
ct_df = ts.get_data_df("analysis/cleaned_trip", time_query=None)

#Get GeoJson for trip
first_trip_for_user = ct_df.iloc[0]
first_trip_start_ts = first_trip_for_user.start_ts
first_trip_end_ts = first_trip_for_user.end_ts
trip_start_end_fuzz = 10 # seconds
trips_geojson_list = gfc.get_geojson_for_ts(test_user_id, first_trip_start_ts-trip_start_end_fuzz, ct_df.iloc[-1].end_ts+trip_start_end_fuzz)
print(len(trips_geojson_list))
map_list = lo.get_maps_for_geojson_trip_list(trips_geojson_list)