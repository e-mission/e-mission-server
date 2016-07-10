__author__ = 'Yin'
# Standard imports
import logging
from uuid import UUID

# Our imports
from emission.analysis.result.carbon import getModeCarbonFootprint, carbonFootprintForMode
from emission.core.common import Inside_polygon,berkeley_area,getConfirmationModeQuery
from emission.core.get_database import get_section_db,get_profile_db
import geojson as gj
import emission.analysis.plotting.geojson.geojson_feature_converter as gfc
import emission.core.wrapper.motionactivity as ecwm
import emission.storage.decorations.timeline as esdt
import emission.storage.decorations.local_date_queries as esdl
import emission.storage.decorations.location_queries as esdlq

import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.section as ecws
import emission.storage.timeseries.geoquery as estg
import emission.storage.timeseries.tcquery as esttc
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.timeseries.aggregate_timeseries as estag

# Note that all the points here are returned in (lng, lat) format, which is the
# GeoJSON format.

def carbon_by_zip(start,end):
    Profiles=get_profile_db()
    carbon_list=[]
    for zip in Profiles.distinct('zip'):
        # print(zip)
        if zip!='N/A':
            tempdict={}
            tempdict['weight']=Profiles.find({'zip':zip}).count()
            # print(Profiles.find({'zip':zip}).count())
            tempdict['carbon']=0
            for profile in Profiles.find({'zip':zip}):
                tempdict['loc']=profile['zip_centroid']
                user=profile['user_id']
                user_carbon=getModeCarbonFootprint(user,carbonFootprintForMode,start,end)
                tempdict['carbon']+=sum(list(user_carbon.values()))
            tempdict['carbon']=tempdict['carbon']/tempdict['weight']
            carbon_list.append(tempdict)
    return {"weightedLoc": carbon_list}

def Berkeley_pop_route(start_ts, end_ts):
    berkeley_json  = {"geometry": {
      "type": "Polygon",
      "coordinates": [[
        [-122.267443, 37.864693], [-122.267443, 37.880687], [-122.250985, 37.880687], [-122.250985, 37.864693], [-122.267443, 37.864693]
        ]]
      }
    }
    # box = [ [-122.267443, 37.864693], [-122.250985, 37.880687] ]
    start_dt = esdl.get_local_date(start_ts, "UTC")
    end_dt = esdl.get_local_date(end_ts, "UTC")
    time_query = esttc.TimeComponentQuery("data.ts", start_dt, end_dt)
    geo_query = estg.GeoQuery(["data.loc"], berkeley_json)
    loc_entry_list = esda.get_entries(esda.CLEANED_LOCATION_KEY, user_id=None,
                                      time_query=time_query,
                                      geo_query=geo_query)
    return {"lnglat": [e.data.loc.coordinates for e in loc_entry_list]}

def range_mode_heatmap(modes, from_ld, to_ld, region):
    time_query = esttc.TimeComponentQuery("data.local_dt", from_ld, to_ld)

    if region is None:
        geo_query = None
    else:
        geo_query = estg.GeoQuery(["data.loc"], region)

    test_phones_query = estag.get_ignore_test_phone_extra_query()
    extra_query_list = [test_phones_query]

    if modes is not None:
        mode_enum_list = [ecwm.MotionTypes[mode] for mode in modes]
        extra_query_list.append(esdlq.get_mode_query(mode_enum_list))

    loc_entry_list = esda.get_entries(esda.CLEANED_LOCATION_KEY, user_id=None,
                                      time_query=time_query, geo_query=geo_query,
                                      extra_query_list=extra_query_list)
    return {"lnglat": [e.data.loc.coordinates for e in loc_entry_list]}



