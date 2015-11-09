import emission.simulation.markov_model_counter as emmc
import emission.net.ext_service.otp.otp as otp
import emission.ce186.extract_campus_points as ecp
import random

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

    def get_top_choice(self, start, end):
        curr_time = datetime.datetime.now()
        curr_month = curr_time.month
        curr_year = curr_time.year
        curr_minute = curr_time.minute
        curr_day = curr_time.day
        curr_hour = curr_time.hour
        mode = "WALKING"
        if self.has_bike:
            mode = "BYCICLE"

        our_otp = otp.OTP(start, end, mode, write_day(curr_month, curr_day, curr_year), write_time(curr_hour, curr_minute), self.has_bike)
        lst_of_trips = our_otp.get_all_trips()

        scores = [ ]

        for trip in lst_of_trips:
            scores.append(self.get_score_for_trip(trip))

        return get_top(lst_of_trips, scores)



    def get_score_for_trip(self, trip):
        """ The bulk of the project, stubbed out for now """
        noises = parse_noise()
        beuties = parse_beauty()
        for section in trip.sections:


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


class Area:

    def __init__(self, name, tl, br, beauty=None, noise=None):
        self.name = name
        self.bounding_box = (tl, br)
        self.beauty = beauty
        self.noise = noise

    def point_in_area(self, lat, lng):
        return ecp.in_bounding_box(lat, lng, self.bounding_box)

    
def parse_noise():
    noise_file = open("emission/ce186/NoiseLatLong.csv")
    noise_areas = [ ]
    for noise_line in noise_file:
        noise_line = noise_line.split()
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
        beauty_line = beauty_line.split()
        name = beauty_line[0]
        tl = (float(beauty_line[1]), float(beauty_line[2]))
        br = (float(beauty_line[5]), float(beauty_line[6]))
        beaty = int(beauty_line[7])
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
 
