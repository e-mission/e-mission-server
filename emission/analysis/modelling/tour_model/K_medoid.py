# Standard imports
import numpy as np
import random

# Our imports
import emission.analysis.modelling.tour_model.trajectory_matching as etm
import emission.core.common as ec

def user_data(user_id, database):
    data_feature = {}

    for section in database.find({'$and':[{'user_id': user_id},{'type': 'move'}]}):
        if len(section['track_points'])< 153:
            data_feature[section['_id']] = etm.route_matching.getRoute(section['_id'])

    return data_feature



def update_dtw(data_feature, DTW):
    ids = data_feature.keys()
    for _id in ids:
        DTW[_id] = {}

    for _id in ids:
        for key in ids:
            try:
                DTW[_id][key]
                DTW[key][_id]
            except KeyError:
                value = etm.DTW.Dtw(data_feature[_id], data_feature[key], ec.calDistance)
                value = value.calculate()/len(value.get_path())
                DTW[_id][key] = value
                DTW[key][_id] = value


def cluster_points(X, mu, DTW):
    clusters  = {}
    for x in X:
        key = bestmukey(x, mu, X, DTW) 
        try:
            clusters[key].append(x)
        except KeyError:
            clusters[key] = [x]
    return clusters


def bestmukey(section_id, cluster_ids, data, DTW):
    key = cluster_ids[0]
    distance = DTW[section_id][cluster_ids[0]]
    
    for _id in cluster_ids:
        try:
            currDistance = DTW[section_id][_id]
        except:
            print section_id
            print len(data[section_id])
            print _id
            print len(data[_id])
            print ''
        if currDistance < distance:
            distance = currDistance
            key = _id
    return key

def reevaluate_centers(mu, clusters, X, DTW):
    newmu = []
    for k in clusters.keys():
        key = k
        cost = Cost(key, clusters[k], X, DTW)
        for _id in clusters[k]:
            currCost = Cost(_id, clusters[k], X, DTW)
            if currCost < cost:
                cost = currCost
                key = _id
        newmu.append(key)
    return newmu

def Cost(section_id, cluster_ids, data, DTW):
    cost = 0
    for _id in cluster_ids:
        try:
            currCost = DTW[section_id][_id]
            cost += currCost
        except:
            print section_id
            print len(data[section_id])
            print _id
            print len(data[_id])
            print ''
    return cost


def has_converged(mu, oldmu):
    return mu == oldmu

def find_centers(X, K, DTW):
    # Initialize to K random centers
    oldmu, mu = [], []
    clusters = {}
    final_clusters = None
    final_mu = None
    cost = float("inf")

    if K == 1:
        mu = X.keys()[0]
        return [[mu], {mu: X.keys()}]

    for i in range(100):
        oldmu = random.sample(X, K)
        mu = random.sample(X, K)
        while not has_converged(mu, oldmu):
            oldmu = mu
            # Assign all points in X to clusters
            clusters = cluster_points(X, mu, DTW)
            # Reevaluate centers
            mu = reevaluate_centers(oldmu, clusters, X, DTW)
        currCost = aveClusterCost(clusters, X, DTW)
        if currCost < cost:
            cost = currCost
            final_clusters = clusters
            final_mu = mu
    return final_mu, final_clusters


def aveClusterCost(clusters, data, DTW):
    cost = 0
    n = 0
    for center in clusters:
        for _id in clusters[center]:
            currCost = DTW[center][_id]
            cost += currCost
            n += 1
    return cost/n


def ClusterCost(cluster, data, DTW):
    cost = 0
    n = 0
    for center in clusters:
        for _id in clusters[center]:
            currCost = DTW[center][_id]
            cost += currCost
            n += 1
    return cost/n


