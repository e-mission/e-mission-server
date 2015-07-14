import random
import sys
import numpy

"""
This file implements the k-medoid clustering algorithm. 

As input, this takes:
- data = the data set to cluster
- k = the number of clusters
- rad = a boolean for whether or not to use the metric which takes into consideration the 100 meter geo-fence. 

This code runs slowly. I am working on making it faster. 

code based on K_medoid_2.py in CFC_WebApp/main

"""
def kmedoids(data, k, rad):
    if k >= len(data):
        return (0, [], {})

    #compute distance matrix
    mat = mat_dist(data, rad)

    #initialize with same random seed each time
    random.seed(8)
    
    size = len(data)


    #indices of beginning medoids
    medoids_idx = random.sample(xrange(size), k)
    
    #initial cost and list of items in each initial cluster
    pre_cost, medoids = totalCost(size, mat, medoids_idx)

    current_cost = pre_cost #cost
    best_choice = [] #best choice of medoids
    best_res = {} #best choice for list of each cluster
    iter_count = 0
    while True:
        for idx in range(len(medoids_idx)):
            m = medoids_idx[idx]
            for item in medoids[m]:
                if item != m:
                    medoids_idx[idx] = item
                    tmp_cost, tmp_medoids = totalCost(size, mat, medoids_idx)
                    if tmp_cost < current_cost:
                        best_choice = list(medoids_idx)
                        best_res = dict(tmp_medoids)
                        current_cost = tmp_cost
                    medoids_idx[idx] = m
        iter_count += 1
        if best_choice == medoids_idx:
            break

        if current_cost <= pre_cost:
            pre_cost = current_cost
            medoids = best_res
            medoids_idx = best_choice

    center_distances = [0] * len(data)
    for key in best_res:
        for val in best_res[key]:
            center_distances[key] = mat[key,val]
    return(current_cost, best_choice, best_res, center_distances)

#compute total cost
def totalCost(size, mat, medoids_idx):
    total_cost = 0.0
    medoids = {}
    for idx in medoids_idx:
        medoids[idx] = []

    for i in range(size):
        choice = -1
        min_cost = sys.maxint

        for m in medoids_idx:
            tmp = mat[m,i]
            if tmp < min_cost:
                choice = m
                min_cost = tmp
        medoids[choice].append(i)
        total_cost += min_cost

    return (total_cost, medoids)

def mat_dist(data, rad):
    size = len(data)
    mat = [0] * size
    for i in range(size):
        mat[i] = [0] * size
    for i in range(size):
        for j in range(i):
            d = dist(i,j,data,rad)
            mat[i][j] = d
            mat[j][i] = d
    return numpy.array(mat)


def norm(a, data):
    dim_a = data[a]
    sum = 0
    for i in range(len(dim_a)):
        sum += abs(dim_a[i])**2
    return sum

def dist(a,b, data, rad):
    dim_a = data[a]
    dim_b = data[b]
    sum = 0
    for i in range(len(dim_a)):
        sum += abs(dim_a[i] - dim_b[i])**4
    if rad:
        start = distance(dim_a[1], dim_a[0], dim_b[1], dim_b[0])
        end = distance(dim_a[3], dim_a[2], dim_b[3], dim_b[2])
        return start + end
    return sum**(1/4.0)

def distance(lat1, lon1, lat2, lon2):
    import math
    R = 6371000
    rlat1 = math.radians(lat1)
    rlat2 = math.radians(lat2)
    lon = math.radians(lon2 - lon1);
    lat = math.radians(lat2-lat1);
    a = math.sin(lat/2.0)**2 + math.cos(rlat1)*math.cos(rlat2) * math.sin(lon/2.0)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c
    if d <= 100:
        return 0
    return .5



