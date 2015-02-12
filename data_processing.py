import os
import json
from pprint import pprint
import numpy as np
from sklearn import linear_model
from matplotlib import pyplot as plt
'''
What we are interested in is called SECTIONS: components of trips
corresponsing to one type of transportation. Thus, we need 
to run RANSAC on each of these.
'''
path = "our_collection_data/android/after_elapsed_time_fix/"
#processed_data = np.zeros((1,2))
total_data = []
for path, direc, filenames in os.walk(path):
    for filename in filenames:
        fr = open(path+filename)
        data = json.load(fr)
        temp_list = []
        for segment in data:
            if segment["type"] == "move":
                #print "Num. Activities: " + str(len(segment['activities']))
                for act in segment['activities']:
                    #print "Num. Points: " +str(len(act['trackPoints']))
                    points = [[point["loc_utc_ts"], point["lon"], point["lat"]]  for point in act["trackPoints"]]
                    points = np.array(points)
                    if np.shape(points)[0] != 0:
                        total_data.append(points)
#print len(total_data)
model = linear_model.LinearRegression()
first_set = total_data[0]
#print np.shape(first_set)
time_stamp = first_set[:,0].reshape(len(first_set[:,0]), 1)
print np.shape(time_stamp)
lon_lat = first_set[:,1:]
print lon_lat
#reshape(len(first_set[:,1]), 1)
print np.shape(lon_lat)
model.fit(time_stamp, lon_lat)
#model.fit(lon_lat, time_stamp)
model_ransac = linear_model.RANSACRegressor(linear_model.LinearRegression())
model_ransac.fit(time_stamp, lon_lat)
inlier_mask = model_ransac.inlier_mask_
outlier_mask = np.logical_not(inlier_mask)
print inlier_mask
#print outlier_mask
line_X = np.arange(-5, 5)
line_y = model.predict(line_X[:, np.newaxis])
line_y_ransac = model_ransac.predict(line_X[:, np.newaxis])

# Compare estimated coefficients
#print("Estimated coefficients (normal, RANSAC):")
#print(model.coef_, model_ransac.estimator_.coef_)

plt.plot(time_stamp[inlier_mask], lon_lat[inlier_mask], '.g', label='Inliers')
plt.plot(time_stamp[outlier_mask], lon_lat[outlier_mask], '.r', label='Outliers')
plt.plot(line_X, line_y, '-k', label='Linear regressor')
plt.plot(line_X, line_y_ransac, '-b', label='RANSAC regressor')
#plt.legend(loc='lower right')
plt.savefig("ransac.png")
#dataDict = {}
#dataDict["points"] =  processed_data
'''
print len(dataDict["points"])
print np.shape(dataDict["points"])
print dataDict["points"][0]
'''
