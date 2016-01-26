import numpy as np

# Constants
DAYS_IN_WEEK = 7
HOURS_IN_DAY = 24


class TourModel(object):

    """ The graph object that represents the common trips for a user of E-Missions """

    def __init__(self, user_id):
        self.timestamp = "timestamp"
        self.user_id = user_id

    def get_id(self):
        """ Returns the user_id of the E-Missions user the graph represents """ 
        return self.user_id

    def get_top_trips(self, n):
        """ Returns a list of the n most common trips for the user """
        pass

    def run_random_walk(self, day, start_time):
        """ Returns a list of CommonPlace objects that represent an average day for the user based on Markov Model probabilites. """
        pass

    def plot_graph(self):
        """ Returns a pythonic image of the TourModel """
        pass

    def plot_on_leaflet(self):
        """ Plots an actual map of the TourModel on an html file that will be in the e-mission-server directory """
        pass

class CommonTrip(object):

    """ 
    A class that represents a "common trip" for an E-Missions user.
    A common trip is a start, end point pair that shows up as a high enough percentage of all their start end pairs.
    """

    def __init__(self, starting_point, ending_point):
        self.starting_point = starting_point
        self.ending_point = ending_point
        self.trips = [] # list of trip_ids
        self.probabilites = np.zeros((DAYS_IN_WEEK, HOURS_IN_DAY))

    def get_time_duration(self):
        """ Returns an estimated time of travel as a datetime.timedelta object """
        pass

    def get_distance(self):
        """ Returns the atcf distance of the trip """
        pass

    def get_weight(self):
        """ Retuns the relative importance of the trip """
        pass

    def get_trip_obj(self, trip_id):
        """ Returns the actual trip wrapper object based on an id from self.trips """
        pass

class CommonPlace(object):

    """ 
    An object that reprents a "common place" for an E-Missions user.
    Chosen if enough of the users trips start or end around here.
    """

    def __init__(self, _id):
        self._id = _id # Some sort of identifiying name, (ie 1, "home", "work")

    def set_coords(self, coords):
        """ Set the approx coords for this place """
        pass

    def get_coords(self):
        """ Get the representative coords for this place """
        pass

    def get_address(self):
        """ Get the geocoded address for this place """
        pass

    def get_successor(self):
        """ Get the next place a user would go based on probabilites """
        pass

    def set_time_duration(self, duration, day):
        """ Set the amounf of time a user would spend here on any day day """
        pass

    def get_time_duration(self, day):
        """ Get the time a user spends here on day day """
        pass