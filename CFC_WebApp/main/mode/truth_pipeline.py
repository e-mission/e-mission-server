"""
# Ground Truth pipeline #

## Data clustering and formatting ##

Note: All of this is done on a per-user basis, but the process can be modified later to be user agnostic

### Import data for user ###

Using the provided UUID's get all section data from each user.

**Users** 
* Shankari
* Shankari's Husband
* Culler
* Zack 

### Cluster data for user ###

* Update the route clusters for each user.
* Create sets of KML files (maps) for EACH of the clusters. A maximum of 10 layers (sections) is allowed per KML, so a cluster may be split between KML files.

**2 Options** 
Option RANDOM: Randomly select representative samples from each cluster
Option HUMAN: Use your good judgement to select the samples that best represent each cluster 

### Option RANDOM ###

Use the __NAME_HERE__ function to generate representative samples based on updated clusters. This function may create multiple KML files if there are more than 10 clusters for a given user.

### Option HUMAN ###

Look through each of the clusters and manually select one or more representative samples. Store the section id for each of the sections in your sample. And list them in a file entitled sampled_section_ids_human.json

## Ground Truthifying ##

For each of the representative trips, open them in MyMaps, and then adjust, add, and delete waypoints until you think the given trip is best represented. __INSERT_HUMANITY_HERE__.

## Import Ground Truth ##

* Place each of the modified kml files into a folder called "ground_truth"
* Run __FUNCTION_NAME__ to import all of those modified KML files into the Stage_routeTruth collection
* Maps will then be created of for each of these modified sections that compare the original section with its ground truth. If any issues are observed, then they can be modified and this importing process can be repeated.

"""
import os, sys, random
sys.path.append("%s/../" % os.getcwd())
from get_database import get_section_db, get_routeCluster_db, get_groundClusters_db
from util import sections_to_kml, chunks, kml_multiple_to_geojson, get_kml_section_ids

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

def update_route_clusters(user):
    from Profile import generate_route_clusters
    generate_route_clusters(user)

def cluster_to_kml(user, cluster, cluster_id):
    """
    Creates a single, or possibly multiple KML files a given cluster.
    A KML file is limited by MyMaps to having only 10 layers, so only 
    10 sections will be in a given KML file.

    Responsibilty of caller to check existence and formatting of cluster 
    """ 
    Sections = get_section_db()
    for i,chunk in enumerate(chunks(cluster,10)):    
        sections = map(lambda section_id: Sections.find_one({'_id':section_id}), chunk)
        sections_to_kml("%s_cluster_data_kml/CLUSTER_%s_%i" % (user, str(cluster_id), i), sections)
        
def all_user_clusters_to_kml(user, user_id):
    """
    Creates KML files for all of a given user's clusters
    """
    user_clusters = get_routeCluster_db().find_one({'$and':[{'user':user_id},{'method':"dtw"}]})
    for idc, cluster in user_clusters['clusters'].items():
        cluster_to_kml(user, cluster, idc)

def read_uuids():
    """
    Reads in UUIDs from the file user_uuid.secret

    Format of file:
    name : UUID\n
    ...
    ...
    """
    from uuid import UUID
    f = open("user_uuid.secret","r")
    user_uuid = {}
    for line in f:
        user, uuid = map(lambda c: c.strip(), line.split(":"))
        user_uuid[user] = UUID(uuid)
    return user_uuid

def __collect(user, user_id):
    all_user_clusters_to_kml(user, user_id)

def __sample_representatives(user, user_id):
    pass
    
def __import_truth(user, user_id):
    directory = "%s_cluster_data_kml" % user
    for cluster in os.listdir(directory):
        print cluster
        path = os.path.join(directory,cluster)
        kml_multiple_to_geojson(path, "%s_import_data_json" % user)

if __name__ == "__main__":
    import argparse
    from uuid import UUID
    user_uuid = read_uuids()
    parser = argparse.ArgumentParser(description='Ground truth')
    parser.add_argument('user', metavar='U', type=str, choices=user_uuid.keys(), 
                        help='Type a user you want to ground truth')
    parser.add_argument('-u', '--update', dest='update', action='store_const',
                        const=True, default=False,
                        help='Update user route clusters')
    parser.add_argument('-f', '--force', dest='force', action='store_const',
                        const=True, default=False,
                        help='Force overwrite of stored data')
    parser.add_argument('-s', '--stage', type=str, choices=['collect', 'sample', 'import'], 
                        help='Optionally select a single pipeline stage')

    args = parser.parse_args()
    user, user_id = args.user, user_uuid[args.user]
    stage = args.stage
    if args.update:
        update_route_clusters(user_id)        

    if stage == 'import':
        __import_truth(user, user_id)
        exit('You can view the generated data in %s_import_data_json' % user)
    elif stage == 'sample':
        exit('Sampling is not supported yet')

    abort = False
    directory = '%s_cluster_data_kml' % user
    dir_exists = os.path.exists(directory)
    if not dir_exists:
        os.makedirs(directory)
    elif dir_exists and not args.force:
        abort = True
        put = raw_input("Are you sure you want to overwrite %s_cluster_data_kml? [Y/n]" % user)
        if put.strip().lower() in ("yes", "y"):
            abort = False
    if abort:
        exit(0)

    if stage == 'collect': 
        __collect(user, user_id)
        exit("You can view the generated data in %s_cluster_data_kml" % user)
    else:
        __collect(user, user_id)
        __sample_representatives(user, user_id)
        __import_truth(user, user_id)
