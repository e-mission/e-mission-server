from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import *
from past.utils import old_div
from pymongo import MongoClient
from uuid import UUID
import time

# Our imports
import emission.analysis.modelling.tour_model as eamt

def pipeline(groundTruth, cluster_func, diff_metric, K_option = 'manual'):
    routeDict = getRouteDict(groundTruth)
    differenceDict = getDifferenceDict(routeDict, diff_metric)
    if K_option == 'manual':
        K = len(groundTruth)
        medoids, clusters = cluster_func(routeDict, K, differenceDict)
        return CCR(clusters, medoids, groundTruth)

def getRouteDict(routeDB):
    #print 'getting route track points ... '
    routeDict = {}
    for cluster in routeDB:
        for _id in cluster['list']:
            routeDict[_id] = eamt.route_matching.getRoute(_id)
    return routeDict

def getDifferenceDict(routeDict, diff_metric = 'DTW'):
    #print 'calculating difference matrix ... '
    ids = list(routeDict.keys())
    differences = {}
    for _id in ids:
        differences[_id] = {}
    for _id in ids:
        for key in ids:
            try:
                differences[_id][key]
                differences[key][_id]
            except KeyError:
                if diff_metric == 'DTW':
                    value = eamt.DTW.Dtw(routeDict[_id], routeDict[key], common.calDistance)
                    value = value.calculate_distance()
                    differences[_id][key] = value
                    differences[key][_id] = value
                if diff_metric == 'newDTW':
                    value = eamt.DTW.dynamicTimeWarp(routeDict[_id], routeDict[key])
                    differences[_id][key] = value
                    differences[key][_id] = value
                if diff_metric == 'DtwSym':
                    value = eamt.DTW.DtwSym(routeDict[_id], routeDict[key], common.calDistance)
                    value = value.calculate_distance()
                    differences[_id][key] = value
                    differences[key][_id] = value
                if diff_metric == 'DtwAsym':
                    value = eamt.DTW.DtwAsym(routeDict[_id], routeDict[key], common.calDistance)
                    value = value.calculate_distance()
                    differences[_id][key] = value
                    differences[key][_id] = value
                if diff_metric == 'LCS':
                    value = eamt.LCS.lcsScore(routeDict[_id], routeDict[key], 2000)
                    differences[_id][key] = value
                    differences[key][_id] = value
                if diff_metric == 'Frechet':
                    value = eamt.Frechet.Frechet(routeDict[_id], routeDict[key])
                    differences[_id][key] = value
                    differences[key][_id] = value
                
    return differences

def listCompare(list1, list2):
    fst = set(list1)
    Snd = set(list2)
    count = 0
    while len(fst) != 0 and len(Snd) != 0:
        elem = fst.pop()
        if elem in Snd:
            count += 1
            Snd.remove(elem)
    return count

def CCR(testClusters, medoids, groundTruthClusters):
    N = sum([len(testClusters[i]) for i in medoids])
    count = 0 
    clusterList = [testClusters[i] for i in medoids]
    for cluster in groundTruthClusters:
        maxcount = 0
        for currList in clusterList:
            currCount = listCompare(currList, cluster['list'])
            if currCount > maxcount:
                maxcount = currCount
        count+= maxcount
    return old_div(float(count),float(N))


if __name__ == '__main__':
    usercluster = MongoClient().Routes.user_groundTruth
    difference_metric = ['DTW'
                        ,'newDTW'
                        ,'DtwSym'
                        , 'DtwAsym'
                        #, 'LCS'
                        #, 'Frechet'
                        ]

    K_value_option = 'manual'
    print('######')
    for user in usercluster.find():
        print('USER ID: ' + str(user['user_id']))
        print('NUMBER OF CLUSTERS: '+ str(len(user['clusters'])))
        print('NUMBER OF SECTIONS: '+ str(user['size']))
        print('') 
        if user == 'artificialRoutes':
            #ccr = pipeline(groundTruth, routeType, K_medoid.find_centers, diff_metric = difference_metric, K_option = K_value_option)
            ccr = 0
        else:
            for metric in difference_metric:
                start = time.time()
                ccr = pipeline(user['clusters'], eamt.kmedoid.find_centers, metric, K_value_option)
                end = time.time()
                print('...DIFFERENCE METRIC: ' + metric)
                print('...CLUSTER CORRECTNESS RATE: ' + str(round(ccr,2)))
                print('...PIPELINE TIME ElAPSED: ' + str(round((end - start)*100, 2)) + 'ms')
                print('')
        print('######')
    
    

