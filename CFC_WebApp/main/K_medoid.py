from __future__ import division
# coding: utf-8

# In[99]:

import numpy as np
from pymongo import MongoClient
#from featurecalc import *
import random
from uuid import UUID
from gmap_display import *
from pygeocoder import Geocoder
from route_matching import *
from DTW import Dtw
from tripManager import calDistance
from matplotlib import pyplot as plt



# In[118]:
"""
RAINBOW = {0:'#000000', 
           1:'#0000FF', 
           2:'#00FF00',
           3:'#008000',
           4:'#FFFF00',
           5:'#00FFFF',
           6:'#40E0D0',
           
           7:'#8A2BE2',
           8:'#FFA500',
           9:'#808000',
           
           10:'#7CFC00',
           11:'#32CD32',
           
           12:'#FFA500',
           
           13:'#FF4500',
           
           14:'#6A5ACD',
           '':'#FF0000'
          }


# In[101]:

confirmed_mode = {1:'walking',
                   2:'running',
                   3:'cycling',
                   4:'transport',
                   5:'bus',
                   6:'train',
                   7:'car',
                   8:'mixed',
                   9:'air',
                   '':'Unknown'
                  }


# In[102]:
"""

def user_data(user_id, database):
    data_feature = {}

    for section in database.find({'$and':[{'user_id': user_id},{'type': 'move'}]}):
        if len(section['track_points'])< 153:
            data_feature[section['_id']] = getRoute(section['_id'])

    return data_feature


# In[103]:

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
                value = Dtw(data_feature[_id], data_feature[key], calDistance)
                value = value.calculate()/len(value.get_path())
                DTW[_id][key] = value
                DTW[key][_id] = value


# In[104]:

def cluster_points(X, mu, DTW):
    clusters  = {}
    for x in X:
        key = bestmukey(x, mu, X, DTW) 
        try:
            clusters[key].append(x)
        except KeyError:
            clusters[key] = [x]
    return clusters


# In[105]:

def bestmukey(section_id, cluster_ids, data, DTW):
    key = cluster_ids[0]
    #distance = Dtw(data[section_id], data[cluster_ids[0]], calDistance)
    #distance = distance.calculate()/len(distance.get_path())
    distance = DTW[section_id][cluster_ids[0]]
    
    for _id in cluster_ids:
        #print section_id
        #print _id
        #print ''
        try:
            #currDistance = Dtw(data[section_id], data[_id], calDistance)
            #currDistance = currDistance.calculate()/len(currDistance.get_path())
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


# In[106]:

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


# In[107]:

def Cost(section_id, cluster_ids, data, DTW):
    cost = 0
    for _id in cluster_ids:
        try:
            #currCost = Dtw(data[section_id], data[_id], calDistance)
            #currCost = currCost.calculate()/len(currCost.get_path())
            currCost = DTW[section_id][_id]
            cost += currCost
        except:
            print section_id
            print len(data[section_id])
            print _id
            print len(data[_id])
            print ''
    return cost


# In[108]:

def has_converged(mu, oldmu):
    return mu == oldmu


# In[109]:

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


# In[110]:

def aveClusterCost(clusters, data, DTW):
    cost = 0
    n = 0
    for center in clusters:
        for _id in clusters[center]:
            #currCost = Dtw(data[center], data[_id], calDistance)
            #currCost = currCost.calculate()/len(currCost.get_path())
            currCost = DTW[center][_id]
            cost += currCost
            n += 1
    return cost/n


# In[111]:

def ClusterCost(cluster, data, DTW):
    cost = 0
    n = 0
    for center in clusters:
        for _id in clusters[center]:
            #currCost = Dtw(data[center], data[_id], calDistance)
            #currCost = currCost.calculate()/len(currCost.get_path())
            currCost = DTW[center][_id]
            cost += currCost
            n += 1
    return cost/n


# In[120]:
"""
DTW = {}
Ryan = '1cc03940-57f5-3e35-a189-55d067dc6460'
shankari = 'b0d937d0-70ef-305e-9563-440369012b39'
shankari_husb = '3a307244-ecf1-3e6e-a9a7-3aaf101b40fa'
sections = MongoClient().Stage_database.Stage_Sections
data = user_data(UUID(shankari), sections)
update_dtw(data)


# In[123]:

K = 60
mu , clusters = find_centers(data, K)
cost = aveClusterCost(clusters, data)

print 'K = ' + str(K)
print 'Average Cost = ' + str(cost)
print ''

for i in range(61, 68):
    currmu , currclusters = find_centers(data, i)
    currcost = aveClusterCost(currclusters, data)
    if currcost < cost:
        clusters = currclusters
        cost = currcost
        K = i
    print 'K = ' + str(i) + ' yields cost of ' + str(currcost)
    print ''


# In[127]:

berkeley = 'uc berkeley, california'
default = Geocoder.geocode(berkeley)[0].coordinates
    
print 'K = ' + str(K)
print 'Average Cost = ' + str(cost)
print ''
n = 0
GMAP = pygmaps.maps(default[0], default[1], 14)
cluster_size = []
E = 0
for key in clusters.keys():
    startPoint = default
    for entry in sections.find({'_id': clusters[key][0]}):
        startPoint = entry['section_start_point']['coordinates']
    gmap = pygmaps.maps(startPoint[0], startPoint[1], 14)
    MODE = {}
    cost = Cost(key, clusters[key], data)/len(clusters[key])
    distances = []
    m = 1
    for _id in clusters[key]:
        for section in sections.find({'_id': _id}):
            mode = section['confirmed_mode']
            try:
                MODE[mode] += 1
            except KeyError:
                MODE[mode] = 1
            drawSection(section, PATH, gmap, RAINBOW[mode])
            #if cost < 100 and len(clusters[key])> 1:
                #drawSection(section, PATH, GMAP, RAINBOW[n%15])
            distances.append(DTW[key][_id])
            m +=1
    #gmap.draw('cluster/cluster_' + str(n) + '.html')
    print '------------------------------------------------------------------------------'
    print 'Cluster Number ' + str(n)
    n += 1
    print 'Cluster Center ' + key
    print 'Cluster Average Cost = ' + str(cost)
    if cost<100 and len(clusters[key])>1:
        E+=1
    print 'Sections in cluster = ' + str(len(clusters[key]))
    cluster_size.append(cost)
    
    for mode in MODE:
        print 'Mode ' + confirmed_mode[mode] + ' : ' + str(MODE[mode]*100/len(clusters[key])) + '%'
    
    #fig = plt.figure()
    #plt.xlim((min(distances),max(distances)))
    #plt.xlabel("Section Cost (m)")
    #lt.ylabel('Section Numbers')
   
    #plt.hist(distances)
    #plt.show()
    
    
    print ''
    print ''
    print ''
    print '------------------------------------------------------------------------------'
#print 'good'
#print E
#rint 'mean'
#rint np.mean(cluster_size)
#GMAP.draw('cluster/All_Clusters.html')


# In[3]:




# In[85]:

print DTW
"""
# In[ ]:



