import random
import sys

#code based on K_medoid_2.py in CFC_WebApp/main
def kmedoids(data, k):
    if k >= len(data):
        return (0, [], {})

    mat = mat_dist(data)

    random.seed(8)
    medoids_idx = random.sample(xrange(len(data)), k)
    
    pre_cost, medoids = totalCost(data, mat, medoids_idx)

    current_cost = pre_cost
    best_choice = []
    best_res = {}
    iter_count = 0
    while True:
        for m in medoids_idx:
            for item in medoids[m]:
                if item != m:
                    idx = medoids_idx.index(m)
                    swap_temp = medoids_idx[idx]
                    medoids_idx[idx] = item
                    tmp_cost, tmp_medoids = totalCost(data, mat, medoids_idx)
                    if tmp_cost < current_cost:
                        best_choice = list(medoids_idx)
                        best_res = dict(tmp_medoids)
                        current_cost = tmp_cost
                    medoids_idx[idx] = swap_temp
        iter_count += 1
        if best_choice == medoids_idx:
            break

        if current_cost <= pre_cost:
            pre_cost = current_cost
            medoids = best_res
            medoids_idx = best_choice

    return(current_cost, best_choice, best_res)

def cluster_num(mat):
    size = len(mat[0])
    max = 0
    a = -1
    b = -1
    num = 0
    check = True
    while check:
        check = False
        for i in range(size):
            for j in range(i):
                if mat[i][j] > max:
                    max = mat[i][j]
                    a = i
                    b = j
        print a
        print b
        print mat[a][b]
        num += 1
        mat[a][b] = 0
        for i in range(size):
            if mat[a][i] < 100:
                mat[a][i] = 0
                mat[i][a] = 0
        for i in range(size):
            if check == True:
                break
            for j in range(i):
                if mat[i][j] != 0:
                    check = True
                    break
    print num


#compute total cost
def totalCost(data, mat, medoids_idx):
    total_cost = 0.0
    medoids = {}
    for idx in medoids_idx:
        medoids[idx] = []

    for i in range(len(data)):
        choice = -1
        min_cost = sys.maxint

        for m in medoids_idx:
            tmp = mat[m][i]
            if tmp < min_cost:
                choice = m
                min_cost = tmp
        medoids[choice].append(i)
        total_cost += min_cost

    return(total_cost, medoids)

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
    return mat


def dist(a,b, data):
    starta = data[a]['trip_start_location']
    enda = data[a]['trip_end_location']
    startb = data[b]['trip_start_location']
    endb = data[b]['trip_end_location']
    dim_a = [starta[0], starta[1], enda[0], enda[1]]
    dim_b = [startb[0], startb[1], endb[0], endb[1]]
    sum = 0
    for i in range(len(dim_a)):
        sum += abs(dim_a[i] - dim_b[i])**4
    return sum**(1/4.0)
