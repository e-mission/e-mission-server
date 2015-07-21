# Standard imports
from datetime import datetime, timedelta
import logging

# Our imports
from emission.core.get_database import get_section_db
import emission.core.wrapper.trip as rt

def get_all_sections(section_id):
    """ Return all sections in the trip that the specified section is a part of
        For example, if this is the section to go to the train station, return all
        sections for the same trip.
        The input is the _id field of the section
    """
    section = rt.Section.section_from_json(get_section_db().find_one({'_id': section_id}))
    allSections = get_section_db().find({"trip_id": section.trip_id})
    return list(allSections)

def get_all_sections_for_user_day(user,year,month,day):
    """ Return all sections in the trip that the specified section is a part of
        For example, if this is the section to go to the train station, return all
        sections for the same trip.
        The input is the _id field of the section
    """
    dayMidnight = datetime(year,month,day,0,0,0)
    nextDayMidnight = dayMidnight + timedelta(days =1)
    sectionIt = get_section_db().find({'user_id': user,
        "section_start_datetime": {"$gt": dayMidnight},
        "section_end_datetime": {"$lt": nextDayMidnight}})
    return [rt.Section.section_from_json(s) for s in sectionIt]

def get_trip_before(section_id):
    """ Return the trip just before the one that this section belongs to.
    """
    section = rt.Section.section_from_json(get_section_db().find_one({'_id': section_id}))
    logging.debug("Found section %s" % section)
    firstSection = rt.Section.section_from_json(get_section_db().find_one({"trip_id": section.trip_id, "section_id": 0}))
    logging.debug("First section %s" % firstSection)
    # First, try to find the seection assuming that data collection was continuous
    prevPlace = rt.Section.section_from_json(get_section_db().find_one({"section_end_datetime": firstSection.start_time}))
    logging.debug("prevPlace %s" % prevPlace)
    # This should be the "place" trip
    if prevPlace is not None:
        logging.debug("prevPlace.section_type = %s" % prevPlace.section_type)
        if prevPlace.section_type != "place":
            return None
        else:
            prevTrip = get_section_db().find_one({"section_end_datetime": prevPlace.start_time})
            return prevTrip
    else:
        assert(False)
    return allSections

def get_bounds(sectionList):
    # Lat and lng are going to be in the range of -180 to 180.
    # So let's pick large positive and negative numbers to initialize them
    min_lat = 999999
    min_lon = 999999
    max_lat = -9999999
    max_lon = -9999999
    for sectionJSON in sectionList:
        section = rt.Section.section_from_json(sectionJSON)
        logging.debug("Testing start point %s " % section.section_start_location)
        if section.section_start_location.lat < min_lat:
            min_lat = section.section_start_location.lat
        if section.section_start_location.lon < min_lon:
            min_lon = section.section_start_location.lon

        logging.debug("Testing end point %s " % section.section_end_location)
        if section.section_end_location.lat > max_lat:
            max_lat = section.section_end_location.lat
        if section.section_end_location.lon > max_lon:
            max_lon = section.section_end_location.lon
    return (rt.Coordinate(min_lat, min_lon), rt.Coordinate(max_lat, max_lon))

def get_center_for_section(sectionJSON):
    """
        Returns a tuple (lat, lon) that can be passsed in to pygmaps to create a map
        centered at the correct location
    """
    return ((sectionJSON["section_start_point"]["coordinates"][1] +
             sectionJSON["section_end_point"]["coordinates"][1])/2,
             (sectionJSON["section_start_point"]["coordinates"][0] +
             sectionJSON["section_end_point"]["coordinates"][0])/2)
