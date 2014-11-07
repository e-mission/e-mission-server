from __future__ import division
from pymongo import MongoClient
import numpy as np
from common import get_first_daily_point, most_common
from get_database import get_section_db, get_profile_db
from sklearn.cluster import DBSCAN
from collections import Counter
from datetime import datetime

def detect_home_2(user_id):
    Sections = get_section_db()
    list_first_pnt = []
    list_home_candidate = []
    for section in Sections.find({'$and': [{'user_id': user_id}, {'section_start_point': {'$ne': None}}]}):
        list_first_pnt.append(section['track_points'][0])

    list_home_candidate = get_first_daily_point(list_first_pnt)
    if len(list_home_candidate) == 0:
        return 'N/A'
    list_home_candidate_cood = []
    for pnt in list_home_candidate:
        list_home_candidate_cood.append(pnt['track_location']['coordinates'])

    list_home_candidate_np = np.asarray(list_home_candidate_cood)
    minlat = np.min(list_home_candidate_np[:, 0])
    minlng = np.min(list_home_candidate_np[:, 1])
    list_home_candidate_np_2 = np.zeros((len(list_home_candidate_np), 2))
    list_home_candidate_np_2[:, 0] = (list_home_candidate_np[:, 0] - minlat) * 89.7
    list_home_candidate_np_2[:, 1] = (list_home_candidate_np[:, 1] - minlng) * 112.7
    db = DBSCAN(eps=0.2, min_samples=3)
    db_fit = db.fit(list_home_candidate_np_2)
    db_labels = db_fit.labels_
    new_db_labels = db_labels[db_labels != -1]
    if new_db_labels != []:
        frequency = Counter(new_db_labels)
        max_fre = frequency.most_common()[0][1]
        new_location = list_home_candidate_np[db_labels != -1]
        label_unique = np.unique(new_db_labels)
        cluster_center = np.zeros((len(label_unique), 3))
        home_list = []
        for label in label_unique:
            sub_location = new_location[new_db_labels == label]
            if len(sub_location) / max_fre >= 0.3:
                temp_center = np.mean(sub_location, axis=0)
                home_list.append(list(temp_center))

        return home_list
    else:
        return [most_common(list_home_candidate, 200)]


def detect_home_from_db_2(user_id):
    Profiles = get_profile_db()
    user_pro = Profiles.find_one({'$and': [{'source': 'Shankari'}, {'user_id': user_id}]})
    return user_pro['home']
