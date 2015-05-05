from get_database import get_section_db
from recommender.trip import Section

def get_all_sections(section_id):
    """ Return all sections in the trip that the specified section is a part of
        For example, if this is the section to go to the train station, return all
        sections for the same trip.
        The input is the _id field of the section
    """
    section = Section.section_from_json(get_section_db().find_one({'_id': section_id}))
    allSections = get_section_db().find({"trip_id": section.trip_id})
    return list(allSections)

def get_trip_before(section_id):
    """ Return the trip just before the one that this section belongs to.
    """
    section = Section.section_from_json(get_section_db().find_one({'_id': section_id}))
    firstSection = Section.section_from_json(get_section_db().find_one({"trip_id": section.trip_id, "section_id": 0}))
    # First, try to find the seection assuming that data collection was continuous
    prevPlace = Section.section_from_json(get_section_db().find_one({"section_end_datetime": firstSection.start_time}))
    # This should be the "place" trip
    if prevPlace is not None:
        if prevPlace.section_type != "place":
            return None
        else:
            prevTrip = get_section_db().find_one({"section_end_datetime": prevPlace.start_time})
            return prevTrip
    else:
        assert(False)
    return allSections

