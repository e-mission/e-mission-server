import math
import matplotlib.pyplot as plt
import numpy

"""
This class creates a histogram of the data organized into 'bins'. 
Two trips are in the same bin if both their start points and end points 
are within 300 meters of each others. This class takes the data as input. 

Each trip in the data must have a 'trip_start_location' and a 
'trip_end_location' field to run. 

An example of how to run this class can be found in the main() method. 
"""
class similarity:
    
    def __init__(self, data):
        self.data = data
        self.size = len(self.data)
        self.bins = []

    #create bins
    def similar(self):
        for a in range(self.size):
            added = False
            for bin in self.bins:
                if self.match(a,bin):
                    bin.append(a)
                    added = True
                    break
            if not added:
                self.bins.append([a])
        self.bins.sort(key=lambda bin: len(bin), reverse=True)

    #check if two trips match
    def match(self,a,bin):
        for b in bin:
            if not self.distance_helper(a,b):
                return False
        return True

    #create the histogram
    def graph(self):
        bars = [0] * len(self.bins)
        num = 0
        for i in range(len(self.bins)):
            bars[i] = len(self.bins[i])
            num += bars[i]
            if num >= self.size/2.0:
                print i
                print num
                num = -num

        N = len(bars)
        index = numpy.arange(N)
        width = .1
        plt.bar(index+width, bars, color='m')
        plt.xlim([0, N])
        plt.savefig('histogram.png')
        plt.show()
        
    #calculate the distance between two trips
    def distance_helper(self, a, b):
        starta = self.data[a]['trip_start_location']
        startb = self.data[b]['trip_start_location']
        enda = self.data[a]['trip_end_location']
        endb = self.data[b]['trip_end_location']

        start = self.distance(starta[1], starta[0], startb[1], startb[0])
        end = self.distance(enda[1], enda[0], endb[1], endb[0])
        if start and end:
            return True
        return False

    @staticmethod
    def distance(lat1, lon1, lat2, lon2, option=False):
        R = 6371000
        rlat1 = math.radians(lat1)
        rlat2 = math.radians(lat2)
        lon = math.radians(lon2 - lon1);
        lat = math.radians(lat2-lat1);
        a = math.sin(lat/2.0)**2 + math.cos(rlat1)*math.cos(rlat2) * math.sin(lon/2.0)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = R * c
        if option==True:
            print d
        if d <= 300:
            return True
        return False

def main():
    from get_database import get_fake_trips_db
    db = get_fake_trips_db()
    trips = db.find()
    data = []
    for trip in trips:
        data.append(trip)
    sim = similarity(data)
    sim.similar()
    sim.graph()
