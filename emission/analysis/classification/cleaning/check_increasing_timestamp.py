import os
import json
from pprint import pprint
import numpy as np
'''
What we are interested in is called SECTIONS: components of trips
corresponsing to one type of transportation. Thus, we need 
to run RANSAC on each of these.
'''
path = "our_collection_data/android/after_elapsed_time_fix/"
processed_data = np.array([])
for path, direc, filenames in os.walk(path):
    for filename in filenames:
        fr = open(path+filename)
        data = json.load(fr)
        for segment in data:
            temp_list = []
            if segment["type"] == "move":
                print "Num. Activities: " + str(len(segment['activities']))
                for act in segment['activities']:
                    print "Num. Points: " +str(len(act['trackPoints']))
                    points = [[point["loc_utc_ts"], (point["lon"], point["lat"])]  for point in act["trackPoints"]]
                    #points = [[point["loc_elapsed_ts"], (point["lon"], point["lat"])]  for point in act["trackPoints"]]
                    points_test_ascending = sorted(points, key=lambda point: point[0])
                    if points != points_test_ascending:
                        raise Exception
                    temp_list.append(points)
dataDict = {}
dataDict["points"] =  processed_data
print len(dataDict["points"])
print np.shape(dataDict["points"])
print dataDict["points"][0]
