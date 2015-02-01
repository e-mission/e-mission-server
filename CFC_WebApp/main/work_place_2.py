__author__ = 'Yin'
from pymongo import MongoClient
from home import detect_home, detect_home_from_db
from home_2 import detect_home_2, detect_home_from_db_2
from common import Is_weekday, get_static_pnts, most_common_2, calculate_appearance_rate, Is_date, Is_place_2, calDistance
from dateutil import parser
from get_database import get_section_db, get_profile_db

def detect_work_office_2(user_id):
    Trips = get_trip_db()
    office_candidate = []
    homes = detect_home_2(user_id)
    if homes == 'N/A':
        return 'N/A'
    else:
        for trip in Trips.find({'$and': [{'user_id': user_id}, {'type': 'place'}, {'place': {'$ne': {}}}]}):
            start_time = trip['trip_start_datetime']
            end_time = trip['trip_end_datetime']
            cood = trip['place']['place_location']['coordinates']
            away_home = 400
            count = 0
            for home in homes:
                if not Is_place_2(cood, home, away_home):
                    count += 1

            if count == len(homes):
                if start_time.isoweekday() in range(1, 6) and start_time.hour >= 5 and start_time.hour <= 22:
                    for num in range(0, int(round((end_time - start_time).total_seconds() / 360))):
                        office_candidate.append(cood)

        if len(office_candidate) > 0:
            office_location = most_common_2(office_candidate, 200)
            return office_location
        return 'N/A'


def detect_daily_work_office_2(user_id, day):
    Trips = get_trip_db()
    office_candidate = []
    homes = detect_home_2(user_id)
    for trip in Trips.find({'$and': [{'user_id': user_id}, {'type': 'place'}, {'place': {'$ne': {}}}]}):
        start_time = trip['trip_start_datetime']
        end_time = trip['trip_end_datetime']
        cood = trip['place']['place_location']['coordinates']
        away_home = 400
        count = 0
        for home in homes:
            if not Is_place_2(cood, home, away_home):
                count += 1

        if count == len(homes):
            if start_time.isoweekday() == day and start_time.hour >= 5 and start_time.hour <= 22:
                for num in range(0, int(round((end_time - start_time).total_seconds() / 360))):
                    office_candidate.append(cood)

    if len(office_candidate) > 0:
        office_location = most_common_2(office_candidate, 200)
        return office_location
    else:
        return 'N/A'


def detect_work_office_from_db_2(user_id):
    Profiles = get_profile_db()
    user_pro = Profiles.find_one({'$and': [{'source': 'Shankari'}, {'user_id': user_id}]})
    return user_pro['work_place']


def detect_daily_work_office_from_db_2(user_id, day):
    Profiles = get_profile_db()
    user_pro = Profiles.find_one({'$and': [{'source': 'Shankari'}, {'user_id': user_id}]})
    return user_pro['work' + str(day)]
