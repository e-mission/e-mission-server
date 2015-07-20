__author__ = 'Yin'
# Standard imports

# Our imports
from emission.analysis.result.carbon import getModeCarbonFootprint, carbonFootprintForMode
from emission.core.common import Inside_polygon,berkeley_area,getConfirmationModeQuery
from emission.core.get_database import get_section_db,get_profile_db

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

def Berkeley_pop_route(start,end):
    Sections = get_section_db()
    list_of_point=[]
    # print(berkeley_area())
    for section in Sections.find({"$and":[{'In_UCB':True },{'type':'move'},{"section_start_datetime": {"$gte": start, "$lt": end}}]}):
        for pnt in section['track_points']:
                list_of_point.append(pnt['track_location']['coordinates'])
    return {"latlng": list_of_point}

def Commute_pop_route(modeId,start,end):
    Sections = get_section_db()
    list_of_point=[]
    # print(berkeley_area())
    commuteQuery = {"$or": [{'commute': 'to'}, {'commute': 'from'}]}
    modeQuery = {"$or": [{'mode': modeId}, getConfirmationModeQuery(modeId)]}
    dateTimeQuery = {"section_start_datetime": {"$gte": start, "$lt": end}}
    for section in Sections.find({"$and":[modeQuery,dateTimeQuery,{'type':'move'}]}):
        if len(section['track_points']) > 5:
          # skip routes that have less than 3 points
          for pnt in section['track_points'][5:-5]:
                  list_of_point.append(pnt['track_location']['coordinates'])

    return {"latlng": list_of_point}





