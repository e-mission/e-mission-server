# Standard imports
import logging
from uuid import UUID

# Our imports
from emission.core.get_database import get_section_db,get_profile_db
import emission.analysis.plotting.geojson.geojson_feature_converter as gfc
import emission.core.wrapper.motionactivity as ecwm
import emission.storage.decorations.timeline as esdt
import emission.storage.decorations.local_date_queries as esdl
import emission.storage.decorations.location_queries as esdlq

import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.section as ecws
import emission.storage.timeseries.geoquery as estg
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.tcquery as esttc
import emission.storage.decorations.analysis_timeseries_queries as esda

import emission.net.usercache.abstract_usercache as enua

import emission.storage.timeseries.aggregate_timeseries as estag
import emission.storage.timeseries.cache_series as estc

MANUAL_INCIDENT_KEY = "manual/incident"

# Example of region that can be used in the queries below
#     berkeley_json  = {"geometry": {
#       "type": "Polygon",
#       "coordinates": [[
#         [-122.267443, 37.864693], [-122.267443, 37.880687], [-122.250985, 37.880687], [-122.250985, 37.864693], [-122.267443, 37.864693]
#         ]]
#       }
#     }

def range_mode_heatmap_local_date(user_uuid, modes, from_ld, to_ld, region):
    time_query = esttc.TimeComponentQuery("data.local_dt", from_ld, to_ld)
    return range_mode_heatmap(user_uuid, modes, time_query, region)

def range_mode_heatmap_timestamp(user_uuid, modes, from_ts, to_ts, region):
    time_query = estt.TimeQuery("data.ts", from_ts, to_ts)
    return range_mode_heatmap(user_uuid, modes, time_query, region)

def range_mode_heatmap(user_uuid, modes, time_query, region):

    if region is None:
        geo_query = None
    else:
        geo_query = estg.GeoQuery(["data.loc"], region)

    extra_query_list = []
    if modes is not None:
        mode_enum_list = [ecwm.MotionTypes[mode] for mode in modes]
        extra_query_list.append(esdlq.get_mode_query(mode_enum_list))

    loc_entry_list = esda.get_entries(esda.CLEANED_LOCATION_KEY, user_id=user_uuid,
                                      time_query=time_query, geo_query=geo_query,
                                      extra_query_list=extra_query_list)
    return {"lnglat": [e.data.loc.coordinates for e in loc_entry_list]}

def incident_heatmap_local_date(user_uuid, modes, from_ld, to_ld, region):
    time_query = esttc.TimeComponentQuery("data.local_dt", from_ld, to_ld)
    return incident_heatmap(user_uuid, modes, time_query, region)

def incident_heatmap_timestamp(user_uuid, modes, from_ts, to_ts, region):
    time_query = estt.TimeQuery("data.ts", from_ts, to_ts)
    return incident_heatmap(user_uuid, modes, time_query, region)

def incident_heatmap(user_uuid, modes, time_query, region):
    """
    Return a list of geojson points with properties for the time and the stress level
    related to incidents. This should not return full entries because that can
    expose the user_id in the aggregate case. Maybe it can return the data part only?
    Or should we put the other entries into the properties?
    :param modes: The modes that we want to query for
    :param time_query: The time query, in either local date or timestamp
    :param region: The region of interest
    :return: list of `incident` objects, with all metadata stripped out
    """

    if region is None:
        geo_query = None
    else:
        geo_query = estg.GeoQuery(["data.loc"], region)

    extra_query_list = []
    if modes is not None:
        mode_enum_list = [ecwm.MotionTypes[mode] for mode in modes]
        extra_query_list.append(esdlq.get_mode_query(mode_enum_list))

    if user_uuid is None:
        incident_entry_list = esda.get_entries(MANUAL_INCIDENT_KEY, user_id=None,
                                          time_query=time_query, geo_query=geo_query,
                                          extra_query_list=extra_query_list)
    else:
        # We don't support aggregate queries on the usercache. And that is
        # actually fine, because we don't expect immediate results for the
        # aggregate case. We just want to query the usercache to ensure that
        # the incidents don't magically disappear just because they got pushed
        # to the server but are not yet processed
        incident_entry_list = estc.find_entries([MANUAL_INCIDENT_KEY], time_query)
    return {"incidents": [e.data for e in incident_entry_list]}
