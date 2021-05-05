from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import range
from builtins import *
import logging

# Our imports
import emission.analysis.modelling.tour_model.prior_unused.truth_pipeline as tp
import emission.core.get_database as edb
import main.gmap_display as mgp

def make_original(s):
    s1 = s.copy()
    if "original_points" in s:
        s1["modified_points"] = s["track_points"]
        s1["track_points"] = s["original_points"]
        s1["original_points"] = s["original_points"]
    return s1

def get_deleted_points(oplist, tplist):
    deleted_list = []
    for i in range(len(tplist)):
        if oplist[i] != tplist[i]:
            deleted_list.append(i)
            del oplist[i]
        else:
            i = i + 1
    return deleted_list

def generate_cluster_comparison(sID_list, outPath):
    mgp.drawSectionsSeparately(sID_list, "%s/smoothing_after" % outPath)

    # modifiedSections = [make_original(s) for s in sID_list if len(s["track_points"]) != len(s["original_points"])]
    modifiedSections = [make_original(s) for s in sID_list]
    mgp.drawSectionsSeparately(modifiedSections, "%s/smoothing_before" % outPath)

    try:
        import os
        os.mkdir("%s/smoothing_compare" % outPath)
    except OSError as e:
        logging.warning("Error %s while creating result directory " % e)


    for s in sID_list:
        sID = s["_id"]
        beforeFileName = "../smoothing_before/%s.html" % sID
        afterFileName = "../smoothing_after/%s.html" % sID

        before_point_count = len(s["track_points"])
        if "original_points" in s:
            after_point_count = len(s["original_points"]) 
            totuple = lambda ca : (ca[0], ca[1])
            tocoordtuple = lambda pt: totuple(pt["track_location"]["coordinates"])
            tplist = [tocoordtuple(pt) for pt in s["track_points"]]
            oplist = [tocoordtuple(pt) for pt in s["original_points"]]
            deleted_points = get_deleted_points(oplist, tplist)
        else:
            after_point_count = "unfiltered"
            deleted_points = "none"

        compareHtml = \
            """<html>
               <title> %s -> %s (Deleted %s) </title>
               <frameset cols="50%%,50%%">
                   <frame src=%s>
                   <frame src=%s>
               </frameset>
               </html>""" % (before_point_count, after_point_count, deleted_points, beforeFileName, afterFileName)
        with open("%s/smoothing_compare/%s.html" % (outPath, sID), "w") as cf:
            cf.write(compareHtml)


clusterDict = tp.__read_user_clusters_text("shankari", "/Users/shankari/cluster_ground_truth/ground_truthed_clusters/manual_ground_truths")
caltrainSID = clusterDict["shankari_mtn_view_to_millbrae"]
print("Found %d sections in ground truth" % len(caltrainSID))
caltrainSIDInDb = [s for s in caltrainSID if edb.get_section_db().count_documents({'_id': s}) == 1]
print("Found %d ground truthed sections in DB" % len(caltrainSIDInDb))

caltrainSectionsInDb = [edb.get_section_db().find_one({"_id": sid}) for sid in caltrainSIDInDb]
generate_cluster_comparison(caltrainSectionsInDb, "/tmp")

