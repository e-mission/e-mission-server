import random

class Location(object):

    def __init__(self, name):
        self.successors = [ ]
        self.days_of_week = [ ]
        self.name = name
        self.times_allowed_one = 0
        self.times_allowed_two = 0
        self.count = 0
        self.count_down = 0
        self.parents_to_probs = { }

    def add_to_successors(self, loc):
        if type(loc) == list:
            self.successors.extend(loc)
        elif type(loc) == Location:
            self.successors.append(loc)
        # loc.times_allowed_one += 1
        # loc.times_allowed_two += 1
        # loc.count += 1

    def get_prob_of_next(self, parent):
        pass


    def get_successors(self):
        return self.successors

    def visit(self):
        self.times_allowed_one -= 1

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

class TourModel(object):

    def __init__(self, home, name):
        self.home = home
        self.name = name
        self.all_locs = set( )
        for loc in self.home.get_successors():
            loc.count -= 1

    def get_tour_model_from(self, place):
        #return _dfs_search(self, place)
        tour_model = [ ]
        orig_place = place 
        curr_node = place
        tour_model.append(curr_node)
        start = True
        while (curr_node != orig_place) or start:
            start = False
            curr_node = get_next_node(curr_node) ## Do a random walk around the FSM, fix this later??
            tour_model.append(curr_node)
        return tour_model

    def get_all_tour_models(self):
        tour_models = [ ]
        for place in self.home.get_successors():
            tour_models.append(self.get_tour_model_from(place))
        return tour_models
        

def get_next_node(place):
    sucessor = choose_random_successor(place.get_successors())
    return sucessor

def choose_random_successor(successors):
    num_successors = len(successors)
    index = random.randint(0, num_successors - 1)
    return successors[index]


def _dfs_search(TM, starting_point):
    s = [ ]
    came_from = { }
    s.append(starting_point)
    curr = None
    while len(s) > 0:
        node = s.pop()
        node.visit()
        TM.all_locs.add(node)
        successors = node.get_successors()
        for v in successors:
            if v.times_allowed_one > 0:
                s.append(v)
                came_from["%s%s" % (v, v.times_allowed_one)] = node
                curr = v
    for node in TM.all_locs:
        node.count_down = node.count
    return _make_path(came_from, curr, starting_point)

def _make_path(came_from, currNode, starting_point):
    if ( ("%s%s" % (currNode, currNode.count_down) ) in came_from) and (currNode.times_allowed_two > 0):
        currNode.count_down -= 1
        currNode.times_allowed_two -= 1
        temp = _make_path(came_from, came_from["%s%s" % (currNode, currNode.count_down+1)], starting_point)
        return temp + [currNode]
    else:
        return [starting_point]




def test_something():
    """ Basic Sanity Check """
    home = Location("home")
    work = Location("work")
    coffee = Location("coffee")
    home.is_home = True
    home.add_to_successors(work)
    work.add_to_successors(coffee)
    tm = TourModel(home, "test")
    tm_from_work = tm.get_tour_model_from(work)
    print tm_from_work

def test_case_one():
    """ 
    Work to coffee to lunch and then back to work; A simple day
    CHECK -- Works
    """
    home = Location("home")
    work = Location("work")
    coffee = Location("coffee")
    lunch = Location("lunch")
    home.add_to_successors(work)
    work.add_to_successors(coffee)
    coffee.add_to_successors(lunch)
    lunch.add_to_successors(work) ## Completes the cycle
    tm = TourModel(home, "work to coffe to lunch to work")
    tm_from_work = tm.get_tour_model_from(work)
    print tm_from_work


def test_case_two():
    """ Work to coffee to work to lunch and then back to work; slightly more complex """
    home = Location("home")
    work = Location("work")
    coffee = Location("coffee")
    lunch = Location("lunch")
    home.add_to_successors(work)
    work.add_to_successors(coffee)
    coffee.add_to_successors(work)
    work.add_to_successors(lunch)
    lunch.add_to_successors(work)
    tm = TourModel(home, "work -> coffee -> work -> lunch -> work")
    tm_from_work = tm.get_tour_model_from(work)
    print tm_from_work


def test_case_three():
    """ Testing multiple tour models """
    ## Weekday schedule 
    home = Location("home")
    work = Location("work")
    coffee = Location("coffee")
    lunch = Location("lunch")
    
    home.add_to_successors(work)
    work.add_to_successors(coffee)
    coffee.add_to_successors(work)
    work.add_to_successors(lunch)
    lunch.add_to_successors(work)

    ## Weekend schedule 
    weekend_home = Location("weekend_home")
    beach = Location("beach")
    restaraunt = Location("restaraunt")
    another_place = Location("another_place")

    home.add_to_successors(weekend_home)
    weekend_home.add_to_successors(beach)
    beach.add_to_successors(restaraunt)
    restaraunt.add_to_successors(weekend_home)

    tm = TourModel(home, "work -> coffee -> work -> lunch -> work && weekend_home -> beach -> restaraunt -> weekend_home")
    all_tms = tm.get_all_tour_models()
    print all_tms



def complicated_tour():
    ## Create locations
    home = Location('home')
    work = Location('work')
    friend = Location('friend')
    store = Location('store')
    soccer = Location('soccer')
    vegtables = Location('vegtables')
    gas = Location('gas')

    ## Set up successors 
    home_successors = [work, friend, store]    
    work_successors = [soccer, vegtables, friend]
    friend_successors = vegtables
    store_successors = [gas, home]
    soccer_successors = [gas, home]
    veg_successors = [gas, home]
    gas_successors = home

    ## Build Free State Machine
    home.add_to_successors(home_successors)
    work.add_to_successors(work_successors)
    friend.add_to_successors(friend_successors)
    store.add_to_successors(store_successors)
    soccer.add_to_successors(soccer_successors)
    vegtables.add_to_successors(veg_successors)
    gas.add_to_successors(gas_successors)

    tm = TourModel(home, "complicted free state machine")

    print tm.get_tour_model_from(home)



def run_all_test():
    test_something()
    test_case_one()
    test_case_two()
    test_case_three()
    complicated_tour()
