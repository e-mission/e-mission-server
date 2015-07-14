from util import Counter, sampleFromCounter

class Location(object):

    """ A class representing a location in a user's tour model. """

    def __init__(self, name):
        self.name = name
        self.successors_counters = [ ] ## A list of counters for each day of the week, or activity TODO decide that  
        for day in xrange(7):
            self.successors_counters.append( Counter( ) )


    def add_to_successors(self, loc, days_of_week, weight=None):
        """ 
        This function adds successor locations to the current location along with the probability that the user will travel to that place next. 

        You can input a dictionary with locations as keys and weights as values
        For instance, if you are most likely to go home after work, and half as likely to go to safeway, 
        and a fourth as likely to go pick up your kids, you would set up the succesors like this:
        
        >>> work = Location('work')
        >>> home = Location('home')
        >>> safeway = Location('safeway')
        >>> pick_up_kids = Location('kids')
        >>> work_successors = {home : 4, safeway : 2, pick_up_kids : 1}
        >>> work.add_to_successors(work_successors)

        If all are equally likely you can simply input them all in a list:
        >>> work_successors = [home, safeway, pick_up_kids]
        >>> work_successors.add_to_successors(work_successors) 

        And if you want to add a place after originally adding locations you can do so by putting in the location and weight:
        >>> burritos = Location('burritos')
        >>> work.add_to_successors(burritos, 100)

        To update the weight of a succesor you can either call add_n_to_weight_of_successor:

        >>> burritos.add_n_to_weight_of_successor(10)

        or you can reset the weight totally 

        >>> pick_up_kids.set_weight_of_successor(100)

        The counter class is a dictionary type class, of which great documentation is provided in util.py. 
        We use it here as an easy way to get the next state based on the historic probability of user going to that place next.

        """
        if isinstance(loc, dict):
            for day in days_of_week:
                for location, w in loc.iteritems():
                    self.successors_counters[day][location] = w
        elif type(loc) == Location:
            if weight is not None:
                self.successors_counters[days_of_week[0]][loc] = weight
            else:
                self.successors_counter[days_of_week[0]][loc] = 1
        elif type(loc) == list:
            for day in days_of_week:
                for location in loc:
                    self.successors_counter[day][location] = 1
        else:
            raise TypeError("You can not input the location as type %s, please use a dictionary, list or single location and weight" % type(loc))

    def add_n_to_weight_of_successor(self, successor, n, days_of_week):
        for day in days_of_week:
            self.successors_counter[day][sucessor] += n

    def set_weight_of_successor(self, sucessor, weight, days_of_week):
        for day in days_of_week:
            self.successors_counter[day][sucessor] = weight

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

class TourModel(object):

    def __init__(self, home, name):
        self.home = home
        self.name = name

    def get_tour_model_from(self, place):
        tour_model = [ ]
        orig_place = place 
        curr_node = place
        tour_model.append(curr_node)
        start = True
        while (curr_node != orig_place) or start:
            start = False
            curr_node = get_next_node(curr_node)
            tour_model.append(curr_node)
        return tour_model

    def get_tour_model_from_home(self):
        return self.get_tour_model_from(self.home)

def get_next_node(place):
    return sampleFromCounter(place.successors_counter)
