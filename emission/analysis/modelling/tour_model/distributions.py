# standard imports
import datetime
import matplotlib
import matplotlib.pyplot as plt
import numpy 

# our imports
import emission.core.get_database as edb
import emission.analysis.modelling.tour_model.cluster_pipeline as cp
from emission.core.wrapper.trip import Trip

def get_start_times(data):
    dist = []
    for d in data:
        time = d.start_time
        time = time.hour + time.minute/60.0 + time.second/3600.0
        dist.append(time)
    dist.sort(reverse=True)
    plt.yticks(numpy.arange(24), ('12 am', '1 am', '2 am', '3 am', '4 am', '5 am', '6 am', '7 am', '8 am', '9 am', '10 am', '11 am', '12 pm', '1 pm', '2 pm', '3 pm', '4 pm', '5 pm', '6 pm', '7 pm', '8 pm', '9 pm', '10 pm', '11 pm'))
    plt.suptitle('Start Times')
    plt.plot(dist)
    plt.xlabel('Data Points')
    plt.ylabel('Times of Day')
    #plt.savefig('starttimes.png')
    plt.show()
    plt.clf()

def get_durations(data):
    dist = []
    for d in data:
        dur = d['duration']
        dist.append(dur)
    plt.suptitle('Durations')
    plt.plot(dist)
    plt.xlabel('Data Points')
    plt.ylabel('Durations (min)')
    plt.savefig('durations.png')
    plt.clf()

def get_distances(data):
    dist = []
    for d in data:
        distance = d['distance']
        dist.append(distance)
    plt.suptitle('Distances')
    plt.xlabel('Data Points')
    plt.ylabel('Distances (m)')
    plt.plot(dist)
    plt.savefig('distances.png')
    plt.clf()

def get_days(data):
    dist = []
    for d in data:
        time = d['section_start_datetime']
        time = time.isoweekday()
        dist.append(time)
            
    plt.suptitle('Days of the Week')
    plt.yticks(numpy.arange(7), ('Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'))
    plt.xlabel('Data Points')
    plt.ylabel('Days')
    plt.plot(dist)
    plt.savefig('days.png')
    plt.clf()    

if __name__=='__main__':
    data = cp.read_data()
    get_start_times(data)
    #get_durations(data)
    #get_distances(data)
    #get_days(data)
    



