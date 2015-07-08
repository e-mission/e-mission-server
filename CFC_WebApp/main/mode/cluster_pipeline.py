import os, sys
sys.path.append("%s/../" % os.getcwd())

from route_matching import update_user_routeDistanceMatrix, update_user_routeClusters
from K_medoid_2 import kmedoids, user_route_data
import math
from get_database import get_section_db,get_profile_db, get_groundClusters_db, get_routeCluster_db
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pygmaps 
from sklearn.cluster import KMeans
from sklearn import manifold
from route_matching import getRoute
from util import read_uuids


"""
Notes

Usage: python cluster_pipeline.py <username>

Username must be associated with UUID in user_uuid.secret 

High level overview:
-This script provides a series of tools to help you evaluate your clustering algorithm across different methods of calculating distance.

-For a particular user that you pass in to this script, we will generate and plot clusters on a 2d-plane using MDS. colors correspond
to a kmedoid generated clusters

-we also compare kmedoid generated clusters to ground truth clusters and returns accuracy score
"""



if not os.path.exists('mds_plots'):
    os.makedirs('mds_plots')



def extract_features(model_name, user_id, method=None, is_ground_truth=False):
    data = None
    if model_name == 'kmeans':
        data = generate_section_matrix(user_id)
        return data
    elif model_name == 'kmedoid':
        data = get_user_disMat(user_id, method=method, is_ground_truth=is_ground_truth)
    return data

def generate_clusters(model_name, data, user_id, method=None, is_ground_truth=False):
    clusters = None
    if model_name == 'kmeans':
        clusters = kmeans(data)
    elif model_name == 'kmedoid':
        clusters = get_user_clusters(user_id, method=method, nClusters=-1, is_ground_truth=is_ground_truth)
    return clusters

def evaluate_clusters():
    pass


#########################################################################################################
                                    # LOW LEVEL ABSTRACTION #
#########################################################################################################


def get_user_sections(user_id):
    sections = list(get_section_db().find({'$and':[{'user_id': user_id},{'type': 'move'}]}))
    return sections

def get_user_disMat(user, method, is_ground_truth=False):
    ## update route clusters:
    print "Generating route clusters for %s" % user
    if is_ground_truth:
        cluster_section_ids = get_ground_truth_sections(user)
        routes_user = user_route_data2(cluster_section_ids)
        user_disMat = update_user_routeDistanceMatrix(str(user) + '_ground_truth',routes_user,step1=100000,step2=100000,method=method)

    else:
        routes_user = user_route_data(user,get_section_db())
        #print(routes_user)

        user_disMat = update_user_routeDistanceMatrix(user,routes_user,step1=100000,step2=100000,method=method)
        print(type(user_disMat))
    return user_disMat

def get_user_clusters(user, method, nClusters, is_ground_truth=False):
    if is_ground_truth:
        routes_user = user_route_data2(user)
    else:
        routes_user = user_route_data(user,get_section_db())

    if nClusters == -1:
        nClusters = int(math.ceil(len(routes_user)/8) + 1)
    clusters_user = kmedoids(routes_user,nClusters,user,method=method)
    #update_user_routeClusters(user,clusters_user[2],method=method)
    return clusters_user



def get_user_list():

    user_list = get_section_db().distinct('user_id')
    return user_list

def plot_cluster_trajectories():

    for cluster_label in clusters:

        sections = clusters[cluster_label]
        section = sections[0]

        start_point = section['track_points'][0]['track_location']['coordinates']
        mymap = pygmaps.maps(start_point[1], start_point[0], 16)
        #mymap = pygmaps.maps(37.428, -122.145, 16)

        for section in sections:
            path = []
            for track_point in section['track_points']:
                coordinates = track_point['track_location']['coordinates']
                #path.append(coordinates)
                path.append((coordinates[1], coordinates[0]))
            #path = [(37.429, -122.145),(37.428, -122.145),(37.427, -122.145),(37.427, -122.146),(37.427, -122.146)]
            mymap.addpath(path,"#00FF00")


        mymap.draw(str(cluster_label) + '_cluster.html')

def plot_mds(clusters, user_disMat, method, user_id, is_ground_truth=False):
    routes_dict = {}
    c = 0
    for key in user_disMat.keys():
        routes_dict[key] = c
        c += 1
    num_routes = len(routes_dict.keys())
    matrix_shape = (num_routes, num_routes) 

    similarity_matrix = np.zeros(matrix_shape)
    for route1 in user_disMat.keys():
        for route2 in user_disMat[route1]:
            route1_index = routes_dict[route1]
            route2_index = routes_dict[route2]

            similarity_matrix[route1_index][route2_index] = user_disMat[route1][route2]
            #similarity_matrix[route2_index][route1_index] = user_disMat[route1][route2]

    seed = np.random.RandomState(seed=3)

    mds = manifold.MDS(n_components=2, max_iter=3000, eps=1e-9, random_state=seed,
                   dissimilarity="precomputed", n_jobs=1)

    reduced_coordinates = mds.fit_transform(similarity_matrix)

    cluster_num = 0
    cleaned_clusters = {}
    for cluster in clusters[2]:
        for route in clusters[2][cluster]:
            #print(route)
            route_index = routes_dict[route]
            if cluster_num in cleaned_clusters:
                cleaned_clusters[cluster_num].append(reduced_coordinates[route_index])
            else:
                cleaned_clusters[cluster_num] = [reduced_coordinates[route_index]]
        cluster_num += 1

    used_colors = []
    cluster_colors = {}
    for cluster_index in cleaned_clusters:
        stop = False
        while not stop:
            random_color = np.random.rand(1)[0]
            if random_color not in used_colors:
                stop = True
                cluster_colors[cluster_index] = random_color
                used_colors.append(random_color)

    plot_colors = []
    x_coords = []
    y_coords = []
    for cluster_index in cleaned_clusters:
        route_coordinates = cleaned_clusters[cluster_index]
        for coord in route_coordinates:
            plot_colors.append(cluster_colors[cluster_index])
            x_coords.append(coord[0])
            y_coords.append(coord[1])


    plt.scatter(x_coords, y_coords, c=plot_colors)

    x1 = np.mean(x_coords) - 10*np.std(x_coords)
    x2 = np.mean(x_coords) + 10*np.std(x_coords)
    y1 = np.mean(y_coords) - 10*np.std(y_coords)
    y2 = np.mean(y_coords) + 10*np.std(y_coords)

    plt.axis((x1, x2, y1, y2))

    if is_ground_truth:
        f_name = 'mds_plots/ground_truth_' + str(user_id) + '_' + method + '.png'
    else:
        f_name = 'mds_plots/' + str(user_id) + '_' + method + '.png'
    
    plt.savefig(f_name)

#K MEANS HELPER FUNCTIONS

def generate_section_matrix(user_id):
    sections = get_user_sections(user_id)
    inv_feature_dict = {0: 'start_lat', 1: 'start_lng', 2: 'end_lat', 3: 'end_lng', 4: 'duration', 5: 'distance'}
    feature_dict = {'start_lat': 0, 'start_lng': 1, 'end_lat': 2, 'end_lng': 3, 'duration': 4, 'distance': 5}

    data = np.zeros((len(sections), len(feature_dict)))

    c = 0
    while c < len(sections):
        section = sections[c]

        start_point = section['track_points'][0]['track_location']['coordinates']
        end_point = section['track_points'][-1]['track_location']['coordinates']
        start_lat = start_point[1]
        start_lng = start_point[0]
        end_lat = end_point[1]
        end_lng = end_point[0]
        duration = section['duration']
        distance = section['distance']
        data[c][feature_dict['start_lat']] = start_lat
        data[c][feature_dict['start_lng']] = start_lng
        data[c][feature_dict['end_lat']] = end_lat
        data[c][feature_dict['end_lng']] = end_lng
        data[c][feature_dict['duration']] = duration
        data[c][feature_dict['distance']] = distance    
        c += 1
    return data

def kmeans(data):
    sections = get_user_sections(user_id)
    k_means = KMeans(init='k-means++', n_clusters=3, n_init=10)
    k_means.fit(data)
    k_means_labels = k_means.labels_
    c = 0
    clusters = {}
    while c < k_means_labels.shape[0]:
        if k_means_labels[c] not in clusters:
            clusters[k_means_labels[c]] = [sections[c]]
        else:
            clusters[k_means_labels[c]].append(sections[c])
        c += 1
    return clusters

def user_route_data2(section_ids):
    data_feature = {}

    # for section in database.find({'$and':[{'user_id': user_id},{'type': 'move'},{'confirmed_mode': {'$ne': ''}}]}):
    for _id in section_ids:
        try:
            data_feature[_id] = getRoute(_id)
        except Exception as e:
            pass
    #print(data_feature.keys())
    return data_feature

#########################################################################################################
                                # END OF LOW LEVEL ABSTRACTION #
#########################################################################################################


#user_list = get_user_list()

user_uuid = read_uuids()
if len(sys.argv) == 2:
    user_id = user_uuid[sys.argv[1]]

print(user_id)

#PARAMETERS
methods = ['dtw', 'lcs', 'Frechet'] #what metrics for distance to use

#EXPERIMENT 1: KMeans with following features: start_lat, start_lng, end_lat, end_lng, duration, distance
"""
print("Working on KMeans with simple features...")
data = extract_features('kmeans', user_id)
clusters = generate_clusters('kmeans', data, user_id)
print("Finished.")
"""
#EXPERIMENT 2-4: KMedoids with various methods of calculating distance between route A and route B

for method in methods:
    print("Working on KMedoid with " + method + " as distance metric.")
    #user_disMat, clusters_user = generate_route_clusters(user_id, method=method, nClusters=-1)
    data = extract_features('kmedoid', user_id, method)
    clusters = generate_clusters('kmedoid', data, user_id, method)
    print(data)
    plot_mds(clusters, data, method, user_id)
    print("Finished " + method + ".")


def get_ground_truth_sections(username, section_collection):
    """
    Returns all of the routes associated with a username's ground truthed sections
    """
    ground_cluster_collection = get_groundClusters_db()
    clusters = ground_cluster_collection.find_one({"clusters":{"$exists":True}})["clusters"]
    ground_truth_sections = []
    get_username = lambda x: x[0].split("_")[0]
    clusters = filter(lambda x: username == get_username(x), clusters.items())
    for key, section_ids in clusters:
        ground_truth_sections.extend(section_ids)
 
    ground_truth_section_data = {}
    for section_id in ground_truth_sections:
        section_data = section_collection.find_one({'_id' : section_id})        
        if section_data is not None:
            ground_truth_section_data[section_data['_id']] = getRoute(section_data['_id'])
        else:
            print("%s not found" % section_id)
    return ground_truth_section_data

"""
methods = ['dtw']
for method in methods:
    print("test")
    data = extract_features('kmedoid', 'jeff', method, is_ground_truth=True)
    print(data)
    clusters = generate_clusters('kmedoid', data, 'jeff', method, is_ground_truth=True)
    plot_mds(clusters, data, method, 'jeff')
"""
