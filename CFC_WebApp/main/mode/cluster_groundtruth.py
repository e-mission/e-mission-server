"""
Portions of the pipeline related to defining 
ground truth for clusters
"""

import os, re
from get_database import get_groundClusters_db, get_routeCluster_db
from util import get_kml_section_ids

def check_named_clusters(path):
    """
    After the user has looked through 
    each of the clusters, cleaned,
    exported, and renamed the clusters
    this code will check that the clusters 
    have proper names.
    """
    if not os.path.exists(path):
        return False, "%s does not exist" % path
    if not os.path.isdir(path):
        return False, "%s is not a directory" % path
    name_search = lambda line: re.search(r'.*_[0-9].kml', line)        
    for f in os.listdir(path):
        fpath = os.path.join(os.getcwd(), f)
        if os.path.isdir(fpath):
            return False, "%s is a directory" % f
        if not name_search(f):
            return False, "%s is not an appropriate name \n must follow .*_[0-9].kml convention" % f
    return True, "cleaned clusters: %i" % len(os.listdir(path))

def update_db_with_clusters(user, infile_path):
    """
    Updates the groundClusters collection with the sections 
    stored in the KML file path

    infile kmls must be of format some_name_for_cluster_X.kml where X is number
    This is checked in check_named_clusters

    Currently this is very inefficient. It replaces a dictionary of 
    ground truth clusters each time the code is run rather than
    inserting new ground truth entries, but we may not even be using this.
    """
    gc_db = get_groundClusters_db();
    c_db = get_routeCluster_db();
    cluster_name = infile_path.split("/")[-1].split(".")[0][:-2]
    cluster_name = "%s_%s" % (user, cluster_name)
    cluster_sids = get_kml_section_ids(infile_path)
    if(gc_db.count() == 0):
        gc_db.insert({"clusters":{}})
    x = gc_db.find_one({"clusters":{"$exists":True}})["clusters"]
    if(cluster_name in x.keys()):
        x[cluster_name] += cluster_sids
    else:
        x[cluster_name] = cluster_sids
    gc_db.remove({"clusters":{"$exists":True}})
    gc_db.insert({"clusters":x})
