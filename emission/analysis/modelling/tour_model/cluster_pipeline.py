# Standard imports
import sys
import math

# Our imports
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.similarity as similarity
import emission.analysis.modelling.tour_model.featurization as featurization
import emission.analysis.modelling.tour_model.representatives as representatives
"""
This file reads the data from the section database, 
removes noise from the data, and clusters is. 

The parameters and clustering methods can be easily changed, 
but based on what works the best, the featurization and clustering 
works as follows. First, the data is read from the database. 
For featurization, each section is representated as a start point 
and an end point. Then, the section is put into bins and the lower 
half of the bins are removed. Then, the data is clustered using 
k-means. The parameter for k is currently tested in a range based 
on the number of elements, but I plan to adjust this range. 

As input, this file accepts an user's uuid from the command line. 

Currently, this file is defaulted not to handle ground truth, but 
for the purpose of tests, a parameter can be changed to collect ground 
truth from the database and compare the clustering to ground truth. 
To change this, in main, change ground_truth to True in the call to 
read_data. 
"""

#read the data from the database. If ground_truth is true, it will 
#take it from the 'color' field of each section in the database. 
def read_data(uuid=None, ground_truth=False):
    sectiondb = edb.get_section_db()
    #sectiondb = edb.get_fake_trips_db()
    if uuid != None:
        sections = sectiondb.find({'user_id' : uuid})
    else:
        sections = sectiondb.find({'section_start_point' : {'$exists' : True}, 'section_end_point' : {'$exists' : True}})
    

    if sections.count() == 0:
        raise KeyError('no sections found')
    data = []
    colors = []
    for section in sections:
        check(section)
        data.append(section)
        if ground_truth:
            colors.append(section['color'])

    if ground_truth:
        indices = [] * len(set(colors))
        for n in colors:
            if n not in indices:
                indices.append(n)
                    
        for i in range(len(colors)):
            colors[i] = indices.index(colors[i])

    return data, colors

#checks that each section has the necessary information
def check(section):
    a = find_in_dict(section, 'section_start_point', 'coordinates')
    b = find_in_dict(section, 'section_end_point', 'coordinates')
    c = find_in_dict(section, 'section_start_datetime')
    if not (a and b and c):
        raise KeyError('Missing information from section '+ str(section))

#helper function for finding an element in a dictionary and 
#making sure it is not None
def find_in_dict(data, key, second_key = None):
    if key not in data:
        return False
    elif data[key] == None:
        return False
    if second_key != None:
        return find_in_dict(data[key], second_key)
    return True;

#put the data into bins and cut off the lower half of the bins
def remove_noise(data, cutoff, radius, colors=None):
    sim = similarity.similarity(data, cutoff, radius, colors=colors)
    sim.bin_data()
    sim.delete_bins()

    newdata = []
    newcolors = []
    for bin in sim.bins:
        for b in bin:
            d = sim.data[b]
            newdata.append(sim.data[b])
            if bool(colors):
                newcolors.append(colors[b])

    if bool(colors):
        indices = [] * len(set(newcolors))
        for n in newcolors:
            if n not in indices:
                indices.append(n)

        for i in range(len(newcolors)):
            newcolors[i] = indices.index(newcolors[i])

    return newdata, newcolors

#cluster the data using k-means
def cluster(data, colors=None):
    feat = featurization.featurization(data, colors=colors)
    m = len(data)
    min = int(math.ceil(m/7.0))
    max = int(math.ceil(m/4.0))
    feat.cluster(min_clusters=min, max_clusters=max)
    if bool(colors):
        feat.check_clusters()
    return feat.clusters, feat.labels, feat.data

def cluster_to_tour_model(data, labels):
    repy = representatives.representatives(data, labels)
    repy.list_clusters()
    repy.reps()
    repy.locations()
    repy.cluster_dict()

def main(uuid=None):
    data, colors = read_data(uuid, ground_truth=False)
    print len(data)
    data, colors = remove_noise(data, .5, 300, colors = colors)
    print len(data)
    n, labels, data = cluster(data, colors=colors)
    print len(data)
    tour_dict = cluster_to_tour_model(data, labels)
    return tour_dict

if __name__=='__main__':
    main()


