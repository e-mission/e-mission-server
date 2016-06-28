# Standard imports
from __future__ import division
import random

# Our imports
from emission.core.get_database import get_routeDistanceMatrix_db,get_section_db
from emission.analysis.modelling.tour_model.trajectory_matching.route_matching import fullMatchDistance,getRoute

Sections=get_section_db()

def user_route_data(user_id, database):
    data_feature = {}

    # for section in database.find({'$and':[{'user_id': user_id},{'type': 'move'},{'confirmed_mode': {'$ne': ''}}]}):
    for section in database.find({'$and':[{'user_id': user_id},{'type': 'move'}]}):
        data_feature[section['_id']] = getRoute(section['_id'])
    #print(data_feature.keys())
    return data_feature


def totalCost(data_feature,disMat2,medoids_idx):
    '''
    Compute the total cost and do the clustering based on certain cost function
    '''
    # Init the cluster
    # print(type(disMat2))
    #print("I am executing totalCost")
    total_cost = 0.0
    medoids = {}
    for idx in medoids_idx:
        medoids[idx] = []
    #print(medoids)
    # Compute the distance and do the clustering
 
    for i in data_feature.keys():
        choice = -1
    	# Make a big number
        min_cost = float('inf')

        for m in medoids_idx:
            #print(disMat2[m].keys())
            disMat2[m][i]
            disMat2[i][m]
            tmp = (disMat2[m][i]+disMat2[i][m])/2
            if tmp < min_cost:
                choice = m
                min_cost = tmp
    	# Done the clustering
    	medoids[choice].append(i)
    	total_cost += min_cost
        #print("TOTAL COST: " + str(total_cost))

    # Return the total cost and clustering
    #print("TOTAL COST")
    #print(total_cost)
    return(total_cost, medoids)


def kmedoids(data_feature, k, user_id,method='lcs'):
    '''
    kMedoids - PAM implemenation
    See more : http://en.wikipedia.org/wiki/K-medoids
    The most common realisation of k-medoid clustering is the Partitioning Around Medoids (PAM) algorithm and is as follows:[2]
    1. Initialize: randomly select k of the n data points as the medoids
    2. Associate each data point to the closest medoid. ("closest" here is defined using any valid distance metric, most commonly Euclidean distance, Manhattan distance or Minkowski distance)
    3. For each medoid m
        For each non-medoid data point o
            Swap m and o and compute the total cost of the configuration
    4. Select the configuration with the lowest cost.
    5. repeat steps 2 to 4 until there is no change in the medoid.
    '''
    
    if k >= len(data_feature):
        return (0, [], {})

    #disMat_user=get_routeDistanceMatrix_db().find_one({'$and':[{'user':user_id},{'method':method}]})['disMat']
    disMat_user = get_routeDistanceMatrix_db(user_id, method)
    #print(len(disMat_user))
    medoids_idx = random.sample([i for i in data_feature.keys()], k)

    pre_cost, medoids = totalCost(data_feature,disMat_user,medoids_idx)

    current_cost = pre_cost
    best_choice = []
    best_res = {}
    iter_count = 0
    #print("medoids idx")
    #print(medoids_idx)
    #print("medoids")
    #print(medoids)
    while True:

        for m in medoids_idx:
            #print("This is length of medoid_idx")
            #print(len(medoids_idx))
            for item in medoids[m]:
                # NOTE: both m and item are idx!
                if item != m:
                    # Swap m and o - save the idx
                    idx = medoids_idx.index(m)
                    # This is m actually...
                    swap_temp = medoids_idx[idx]
                    medoids_idx[idx] = item
                    tmp_cost, tmp_medoids = totalCost(data_feature,disMat_user,medoids_idx)
                    #print("inside here")
                    #print(len(medoids_idx))
                    # Find the lowest cost
                    if tmp_cost < current_cost:
                        best_choice = list(medoids_idx) # Make a copy
                        best_res = dict(tmp_medoids) 	# Make a copy
                        current_cost = tmp_cost
                    # Re-swap the m and o
                    medoids_idx[idx] = swap_temp
        #print("This is length of medoid_idx")
        #print(len(medoids_idx))
        # Increment the counter
        iter_count += 1

        # print('current_cost: ', current_cost)
        # print('iter_count: ', iter_count)

        if best_choice == medoids_idx:
            # Done the clustering
            break

        # Update the cost and medoids
        if current_cost <= pre_cost:
            pre_cost = current_cost
            medoids = best_res
            medoids_idx = best_choice

    return(current_cost, best_choice, best_res)



