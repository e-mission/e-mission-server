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
        fpath = os.path.join(os.getcwd, f)
        if os.path.isdir(fpath):
            return False, "%s is a directory" % f
        if not name_search(f):
            return False, "%s is not an appropriate name \n must follow .*_[0-9].kml convention" % f

def update_dbs_with_cluster(infile_path):
    gc_db = get_groundClusters_db();
    c_db = get_routeCluster_db();
    cluster_name = infile_path.split("/")[-1].split(".")[0][:-2] # infile kmls must be of format some_name_for_cluster_X.kml where X is number
    cluster_sids = get_kml_section_ids(infile_path)
    if(gc_db.count() == 0):
        gc_db.collection.insert({"clusters":{}})
    x = gc_db.collection.find_one({"clusters":{"$exists":True}})["clusters"]
    if(cluster_name in x.keys()):
        x[cluster_name] += cluster_sids
    else:
        x[cluster_name] = cluster_sids
    c_db = c_db.find({"clusters":{"$exists":True}})
    for db in c_db:
        y = db["clusters"]
        for c in cluster_sids:
            for key, items in y.items():
                if c in items:
                    items.remove(c)
