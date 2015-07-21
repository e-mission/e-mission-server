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
# Standard imports
import os, sys, random

# Our imports
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.prior_unused.util as eaut
import emission.analysis.modelling.tour_model.prior_unused.cluster_groundtruth as eacg

def update_route_clusters(user):
    from Profile import generate_route_clusters
    print "generating clusters for user %s" % user
    generate_route_clusters(user, 20)

def cluster_to_kml(user, cluster, cluster_id):
    """
    Creates a single, or possibly multiple KML files a given cluster.
    A KML file is limited by MyMaps to having only 10 layers, so only 
    10 sections will be in a given KML file.

    Responsibilty of caller to check existence and formatting of cluster 
    """ 
    Sections = edb.get_section_db()
    for i,chunk in enumerate(eaut.chunks(cluster,10)):    
        sections = map(lambda section_id: Sections.find_one({'_id':section_id}), chunk)
        eaut.sections_to_kml("%s_cluster_data_kml/CLUSTER_%s_%i" % (user, str(cluster_id), i), sections)
        
def all_user_clusters_to_kml(user, user_id):
    """
    Creates KML files for all of a given user's clusters
    """
    user_clusters = edb.get_routeCluster_db().find_one({'$and':[{'user':user_id},{'method':"dtw"}]})
    num_clusters = len(user_clusters['clusters'].items())

    print("Writing " + str(num_clusters) + " clusters to disk for " + user + ".")
    for idc, cluster in user_clusters['clusters'].items():
        cluster_to_kml(user, cluster, idc)

def __collect(user, user_id):
    all_user_clusters_to_kml(user, user_id)

def __sample_representatives(user, user_id):
    pass

def __read_user_clusters_kml(user):
    """
    Imports cleaned trip clusters
    
    Assumes that cleaned clusters are placed
    into a directory called 
    USER_clusters_cleaned 
    where USER is the name associated with the 
    user in the user_uuid file

    Assumes that this directory is in the 
    current directory
    """
    path = os.path.join(os.getcwd(), "%s_clusters_cleaned" % user)
    success, message = eacg.check_named_clusters(path)
    if not success:
        exit(message)
    print message
    for kml in os.listdir(path):
        infile_path = os.path.join(path, kml)
        eacg.update_db_with_clusters(user, infile_path)

def __read_user_clusters_text(user, path):
    """
    Reads cleaned user clusters from a text file 
    of the format.
    
    trip_name_1:
    section_id_1
    section_id_2
    section_id_n
    trip_name_2:
    ...    
    """
    eacg.check_cluster_textfile(path)
    cluster_file = open(path, "r")
    clusters = {}
    for l in cluster_file:
        sl = l.strip()
        # Support blank lines by skipping them
        if len(sl) == 0:
            continue
        if ':' in sl:
            name = sl.split(':')[0].strip()
            name = "%s_%s" %(user, name)
            clusters[name] = []
        else:
            section_id = sl.strip()
            clusters[name].append(section_id)
    return clusters

def __read_and_update_clusters_text(user):
    """
    Reads cleaned user clusters from a text file 
    of the format.
    
    trip_name_1:
    section_id_1
    section_id_2
    section_id_n
    trip_name_2:
    ...    
    """
    path = os.path.join(os.getcwd(), "%s_cleaned_clusters.txt" % user)
    clusters = _read_user_clusters_text(user, path)
    eacg.update_db_with_clusters_dict(user, clusters)

def __import_truth(user, user_id):
    directory = "%s_cluster_data_kml" % user
    for cluster in os.listdir(directory):
        path = os.path.join(directory,cluster)
        eaut.kml_multiple_to_geojson(path, "%s_import_data_json" % user)

if __name__ == "__main__":
    import argparse
    from uuid import UUID
    user_uuid = eaut.read_uuids()
    parser = argparse.ArgumentParser(description='Ground truth')
    parser.add_argument('user', metavar='U', type=str, choices=user_uuid.keys(), 
                        help='Type a user you want to ground truth')
    parser.add_argument('-u', '--update', dest='update', action='store_const',
                        const=True, default=False,
                        help='Update user route clusters')
    parser.add_argument('-f', '--force', dest='force', action='store_const',
                        const=True, default=False,
                        help='Force overwrite of stored data')
    parser.add_argument('-s', '--stage', type=str, choices=['collect', 'sample', 'import', 'read_kml', 'read_text'], 
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
    elif stage == 'read_kml':
        __read_user_clusters_kml(user)
    elif stage == 'read_text':
        __read_and_update_user_clusters_text(user)
    else:
        __collect(user, user_id)
        __sample_representatives(user, user_id)
        __import_truth(user, user_id)
