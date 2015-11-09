import emission.simulation.markov_model_counter as emmc
import emission.net.ext_service.otp.otp as otp
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
        return random.randint(1000)

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

    def __init__(self, name, tl, tr, bl, br):
        self.name = name
        self.bounding_box = (tl, tr, bl, br)




def in_bounding_box(lat, lng):
    return True