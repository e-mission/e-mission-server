import emission.simulation.markov_model_counter as emmc
import emission.net.ext_service.otp.otp as otp
import emission.core.our_geocoder as geo
import emission.core.wrapper.trip_old as to

import datetime

class UserModel:

    def __init__(self, name, has_bike=False):
        self.name = name
        self.utilities = emmc.Counter()
        self.has_bike = has_bike

        ## Initialize utilities
        self.utilities["sweat"] = 1
        self.utilities["scenery"] = 1
        self.utilities["social"] = 1
        self.utilities["time"] = 1
        self.utilities["noise"] = 1

    def get_top_choice_places(self, start_place, end_place):
        our_geo = geo.Geocoder()
        start = our_geo.geocode(start_place)
        end = our_geo.geocode(end_place)
        return self.get_top_choice_lat_lng(start, end)

    def get_top_choice_lat_lng(self, start, end):
        curr_time = datetime.datetime.now()
        curr_month = curr_time.month
        curr_year = curr_time.year
        curr_minute = curr_time.minute
        curr_day = curr_time.day
        curr_hour = curr_time.hour
        mode = "WALK"
        if self.has_bike:
            mode = "BICYCLE"

        our_geo = geo.Geocoder()

        print "start is %s" % our_geo.reverse_geocode(start.get_lat(), start.get_lon())
        print "end is %s" % our_geo.reverse_geocode(end.get_lat(), end.get_lon())

        our_otp = otp.OTP(start, end, mode, write_day(curr_month, curr_day, curr_year), write_time(curr_hour, curr_minute), self.has_bike)
        lst_of_trips = our_otp.get_all_trips(0,0,0)

        scores = [ ]

        for trip in lst_of_trips:
            scores.append(self.get_score_for_trip(trip))

        return self.get_top(lst_of_trips, scores)

    def get_score_for_trip(self, trip):
        noises = parse_noise()
        beauties = parse_beauty()

        noises = normalize_scores(noises)
        beauties = normalize_scores(beauties)
        noise_score, beauty_score = 0, 0

        print "length of trip.sections is %s" % len(trip.sections)


        for section in trip.sections:
            print "length of section.points is %s" % len(section.points)
            for point in section.points:
                print 
                print point
                noise_score += get_noise_score(point.get_lat(), point.get_lon(), noises)
                beauty_score += get_beauty_score(point.get_lat(), point.get_lon(), beauties)

        top_score = self.utilities['noise']*noise_score + self.utilities['scenery']*beauty_score
        print "top score is %s" % 
        top_score
        return top_score


    def get_top(self, lst_of_trips, scores):
        arg_max = 9999999999
        max_score = 0
        i = 0
        while i < len(scores):
            if scores[i] > max_score:
                arg_max = i
                max_score = scores[i]
            i += 1
        return lst_of_trips[arg_max]

    def increment_utility(self, which):
        self.utilities[which] += 1

    def increase_utility_by_n(self, which, n):
        for i in xrange(n):
            self.increment_utility(which)

    def normalize_utilities(self):
        self.utilities.normalize()


class Area:

    def __init__(self, name, tl, br, beauty=None, noise=None):
        self.name = name
        self.bounding_box = (tl, br)
        self.beauty = beauty
        self.noise = noise

    def point_in_area(self, lat, lng):
        return in_bounding_box(lat, lng, self.bounding_box)


def in_bounding_box(lat, lon, bounding_box):
    return bounding_box[1][0] <= lat and lat <= bounding_box[0][0] and bounding_box[0][1] <= lon and lon <= bounding_box[1][1]
    
def parse_noise():
    noise_file = open("emission/ce186/NoiseLatLong.csv")
    noise_areas = [ ]
    for noise_line in noise_file:
        noise_line = noise_line.split(',')
        name = noise_line[0]
        tl = (float(noise_line[1]), float(noise_line[2]))
        br = (float(noise_line[3]), float(noise_line[4]))
        noise = int(noise_line[5])
        a = Area(name, tl, br, noise=noise)
        noise_areas.append(a)
    return noise_areas

def parse_beauty():
    beauty_file = open("emission/ce186/beauty.csv")
    beauty_areas = [ ]
    for beauty_line in beauty_file:
        beauty_line = beauty_line.split(',')
        name = beauty_line[0]
        tl = (float(beauty_line[1]), float(beauty_line[2]))
        br = (float(beauty_line[5]), float(beauty_line[6]))
        beauty = int(beauty_line[9])
        a = Area(name, tl, br, beauty=beauty)
        beauty_areas.append(a)
    return beauty_areas

def get_noise_score(lat, lng, noises):
    tot = 0
    for noise_area in noises:
        tot += noise_area.noise
        if noise_area.point_in_area(lat, lng):
            return noise_area.noise
    return float(tot) / float(len(noises)) ## if point isnt in any mapped area return the average

def normalize_scores(areas):
    counter = emmc.Counter()
    print "len of areas is %s" % len(areas)
    for area in areas:
        if area.beauty:
            counter[area.name] = area.beauty
        elif area.noise:
            counter[area.name] = area.noise
    counter.normalize()
    
    new_areas = [ ]
    for name, value in counter.iteritems():
        for area in areas:
            if area.name == name:
                if area.beauty:
                    new_area = Area(name, area.bounding_box[0], area.bounding_box[1], beauty=value)
                elif area.noise:
                    new_area = Area(name, area.bounding_box[0], area.bounding_box[1], noise=value)
                new_areas.append(new_area)

    return new_areas
 
def get_beauty_score(lat, lng, beauties):
    tot = 0
    for beauty_area in beauties:
        tot += beauty_area.beauty
        if beauty_area.point_in_area(lat, lng):
            return beauty_area.beauty
    return float(tot) / float(len(beauties)) ## if point isnt in any mapped area return the average


def test():
    josh = UserModel("josh")
    josh.increase_utility_by_n("scenery", 20)
    josh.increase_utility_by_n("noise", 10)
    top_choice = josh.get_top_choice_lat_lng(to.Coordinate(37.8691323,-122.2549288), to.Coordinate(37.8755814,-122.2589025))
    print_path(top_choice)

def print_path(trip):
    print len(trip.sections)
    for section in trip.sections:
        for point in section.points:
            print point.get_lat(), point.get_lon()
    print trip.trip_end_location.get_lat(), trip.trip_end_location.get_lon()



def write_day(month, day, year):
    return "%s-%s-%s" % (month, day, year)

def write_time(hour, minute):
    return "%s:%s" % (hour, minute)