# standard imports
import random
import sys
import numpy

"""
This file implements the k-medoid clustering algorithm. 

As input, this takes:
- data: the data set to cluster, as a list of four-dimensional points. 
- k: the number of clusters

This code runs slowly. I am working on making it faster. 

code based on K_medoid_2.py in CFC_WebApp/main
The changes that I made were a few small changes to make 
the code run faster and provide a way to calculate and store 
the distance matrix. 
"""

#cluster based on the k-medoids algorithm
def kmedoids(data, k):
    if k >= len(data):
        return (0, [], {})

    #compute distance matrix
    mat = mat_dist(data)

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

#build the distance metric
def mat_dist(data):
    size = len(data)
    mat = [0] * size
    for i in range(size):
        mat[i] = [0] * size
    for i in range(size):
        for j in range(i):
            d = dist(i,j,data)
            mat[i][j] = d
            mat[j][i] = d
    return numpy.array(mat)

#compute the distance between two points
def dist(a,b, data):
    dim_a = data[a]
    dim_b = data[b]
    sum = 0
    for i in range(len(dim_a)):
        sum += abs(dim_a[i] - dim_b[i])**4
    return sum**(1/4.0)

