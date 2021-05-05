"""
Portions of the pipeline related to defining 
ground truth for clusters
"""
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import os, re

# Our imports
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.prior_unused.util as etmu

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

def check_cluster_textfile(path):
    """
    Checks that the ground truthed cluster textfile 
    is in the appropriate format

    trip_name_1:
    section_id_1
    section_id_2
    section_id_n
    trip_name_2:
    ...        
    """
    assert (os.path.isfile(path)), '%s does not exist' % path
    cluster_file = open(path, 'r')
    in_cluster = False
    names = []
    section_id_search = lambda line: re.search(r'^\w{8}-(\w{4}-){3}\w{12}_\w{15}-\w{4}_\w$', line) # Matches format of section_id
    for l in cluster_file:
        l = l.strip()        
        # Support blank lines by skipping them
        if len(l) == 0:
            continue
        if ':' in l:
            assert (not in_cluster), '%s must have at least one section per cluster' % l
            assert (l.split(':')[1].strip() == ''), '%s nothing should follow a colon' % l
            assert (' ' not in l), '%s cluster names must not have spaces' % l
            name = l.split(':')[0].strip()
            assert (name not in names), '%s is not a unique name' % name
            names.append(name)
            in_cluster = True
        else:            
            assert (' ' not in l), '%s section should not have spaces' % l
            assert (section_id_search(l)), '%s is an invalid section id' % l
            in_cluster = False              

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
    gc_db = edb.get_groundClusters_db();
    cluster_name = infile_path.split("/")[-1].split(".")[0][:-2]
    cluster_name = "%s_%s" % (user, cluster_name)
    cluster_sids = etmu.get_kml_section_ids(infile_path)
    if(gc_db.estimated_document_count() == 0):
        gc_db.insert({"clusters":{}})
    x = gc_db.find_one({"clusters":{"$exists":True}})["clusters"]
    if(cluster_name in list(x.keys())):
        x[cluster_name] += cluster_sids
    else:
        x[cluster_name] = cluster_sids
    gc_db.remove({"clusters":{"$exists":True}})
    gc_db.insert({"clusters":x})

def update_db_with_clusters_dict(user, clusters):
    """
    Updates the groundClusters collection with the sections 
    represented in the clusters dict

    Currently this is very inefficient. It replaces a dictionary of 
    ground truth clusters each time the code is run rather than
    inserting new ground truth entries, but we may not even be using this.
    """
    gc_db = edb.get_groundClusters_db();
    assert (clusters != {}), "clusters must be nonempty"
    if(gc_db.estimated_document_count() == 0):
        gc_db.insert({"clusters":{}})
    x = gc_db.find_one({"clusters":{"$exists":True}})["clusters"]
    for name, sections in list(clusters.items()):         
        x[name] = sections # There is likely better way to merge dictionaries
    gc_db.remove({"clusters":{"$exists":True}})
    gc_db.insert({"clusters":x})
