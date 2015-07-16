import similarity
from get_database import get_section_db
import featurization
import sys

"""
Cluster pipeline: Reads the data from the database and forms the clusters. 

"""

def read_data(uuid):
    sectiondb = get_section_db()
    sections = sectiondb.find({'user_id' : uuid})
    if sections.count() == 0:
        raise Exception('No sections found for user ' + str(uuid))
    data = []
    for section in sections:
        check(section)
        start = section['section_start_point']['coordinates']
        end = section['section_end_point']['coordinates']
        data.append({'start' : start, 'end' : end})
    return data

def check(section):
    a = find_in_dict(section, 'section_start_point', 'coordinates')
    b = find_in_dict(section, 'section_end_point', 'coordinates')
    if not a and b:
        raise Exception('Missing information from section '+ str(section))

def find_in_dict(data, key, second_key = None):
    if key not in data:
        return False
    elif data[key] == None:
        return False
    if second_key != None:
        return find_in_dict(data[key], second_key)
    return True;

def remove_noise(data, colors=None):
    sim = similarity.similarity(sectiondata)
    sim.similar()
    n = int(math.ceil(len(sim.bins)/2.0))
    newdata = []
    newcolors = []
    for i in range(n):
        bin = sim.bins[i]
        for b in bin:
            d = sim.data[b]
            newdata.append(sim.data[b])
            if colors != None:
                newcolors.append(colors[b])

    if colors != None:
        indices = [] * len(set(newcolors))
        for n in newcolors:
            if n not in indices:
                indices.append(n)

        for i in range(len(newcolors)):
            newcolors[i] = indices.index(newcolors[i])

    return newdata

def cluster(data, colors=None, ground_truth=False):
    feat = featurization.featurization(data = data, colors=colors, ground_truth=ground_truth)
    m = len(set(feat.colors)) #for now, when we have ground truth
    feat.cluster(min_clusters=2, max_clusters=80)
    feat.check_clusters()


if __name__=='__main__':
    
    if len(sys.argv) != 2:
        print "Please provide a uuid."
        sys.exit(0)

    uuid = sys.argv[1]
    data = read_data(uuid)
    data = remove_noise(data)
    cluster(data)
