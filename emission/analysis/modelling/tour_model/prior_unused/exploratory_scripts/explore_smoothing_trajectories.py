from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import json
import logging
import numpy as np
import datetime as pydt

# Our imports
import emission.analysis.modelling.tour_model.prior_unused.truth_pipeline as tp
import emission.core.get_database as edb
import emission.analysis.section_features as fc
import emission.plotting.gmaps.gmap_display as mgp
import emission.analysis.modelling.tour_model.prior_unused.exploratory_scripts.generate_smoothing_from_ground_truth_clusters as gsfgtc

query = {'type': 'move',
         'confirmed_mode': {'$ne': 9},
         'section_start_datetime' : {'$gt': pydt.datetime(2015, 0o2, 14)},
         '$expr': 'this.track_points.length>1'}

# Now find other sections that meet this criterion
# Manually, we pick the sections with the top 20 average speeds that are not air
def find_other_sections_manual(needsSmoothing, findWithoutSmoothing):
    section_list = []
    maxSpeed_list = []

    for section in edb.get_section_db().find(query):
        avg_speed = fc.calAvgSpeed(section)
        if len(maxSpeed_list) == 0 or fc.calAvgSpeed(section) > max(maxSpeed_list):
            maxSpeed_list.append(avg_speed)
            section_list.append(section)

    return section_list

def get_feature_row(section):
    ret_arr = np.zeros((5))
    ret_arr[0] = fc.calAvgSpeed(section)
    ret_arr[1] = fc.getIthMaxSpeed(section, 1)
    percentiles = np.percentile(fc.calSpeeds(section), [90, 95, 99])
    ret_arr[2] = percentiles[0]
    ret_arr[3] = percentiles[1]
    ret_arr[4] = percentiles[2]
    return ret_arr

def find_other_sections_auto(needsSmoothing, fineWithoutSmoothing):
    from sklearn import tree

    section_list = []

    nPos = len(needsSmoothing)
    nNeg = len(fineWithoutSmoothing)
    nRows = nPos + nNeg
    nCols = 5
    training_feature_set = np.zeros((nRows, nCols))
    result_vector = np.zeros(nRows)

    for (i, section) in enumerate(needsSmoothing):
        training_feature_set[i] = get_feature_row(section)
        result_vector[i] = 1

    for (i, section) in enumerate(fineWithoutSmoothing):
        training_feature_set[nPos + i] = get_feature_row(section)
        result_vector[nPos + i] = -1

    nTestSetRows = get_section_db().count_documents(query)
    test_feature_set = np.zeros((nTestSetRows, nCols))

    testSection_list = []
    for (i, section) in enumerate(get_section_db().find(query)):
        test_feature_set[i] = get_feature_row(section)
        testSection_list.append(section)

    clf = tree.DecisionTreeClassifier()
    clf = clf.fit(training_feature_set, result_vector)
    predictions = clf.predict(test_feature_set)

    for (i, section) in enumerate(testSection_list):
        if predictions[i] == 1:
            section_list.append(section) 

    return section_list

def generate_stats_for_candidates(sID_list):
    pass

def plot_instances_for_gps_error_model():
    smoothing_ground_truth_map = json.load(open("/Users/shankari/cluster_ground_truth/smoothing/caltrain/smoothing_removed_points"))
    needsSmoothing = []
    fineWithoutSmoothing = []

    for (sid, rp_list) in smoothing_ground_truth_map.items():
        sectionJSON = get_section_db().find_one({"_id": sid})
        if sectionJSON is None:
            print("Unable to find section %s in the database" % sid)
        else:
            if len(rp_list) > 0:
                needsSmoothing.append(sectionJSON)
            else:
                fineWithoutSmoothing.append(sectionJSON)

    print("-" * 20, "Needs smoothing", '-' * 20)

    for section in needsSmoothing:
        if section is not None:
            print(section["_id"], fc.calAvgSpeed(section), fc.getIthMaxSpeed(section, 1), np.percentile(fc.calSpeeds(section), [90, 95, 99]))

    print("-" * 20, "Fine without smoothing", '-' * 20)

    for section in fineWithoutSmoothing:
        if section is not None:
            print(section["_id"], fc.calAvgSpeed(section), fc.getIthMaxSpeed(section, 1), np.percentile(fc.calSpeeds(section), [90, 95, 99]))

    other_manual_candidates = find_other_sections_manual(needsSmoothing, fineWithoutSmoothing)
    other_auto_candidates = find_other_sections_auto(needsSmoothing, fineWithoutSmoothing)

    print(other_auto_candidates)

    gsfgtc.generate_cluster_comparison(other_manual_candidates, "/tmp/other_manual")
    gsfgtc.generate_cluster_comparison(other_auto_candidates, "/tmp/other_auto")

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    plot_instances_for_gps_error_model()
