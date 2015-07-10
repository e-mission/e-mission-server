import Queue

class Location(object):

    def __init__(self, name):
        self.successors = [ ]
        self.days_of_week = [ ]
        self.is_home = (name == "home")
        self.name = name
        self.times_allowed_one = 0
        self.times_allowed_two = 0
        self.count = 0
        if self.is_home or name == "work":
            self.count = -1
        self.count_down = 0

    def add_to_successors(self, loc):
        self.successors.append(loc)
        loc.times_allowed_one += 1
        loc.times_allowed_two += 1
        loc.count += 1


    def works_this_day(self, day):
        return day in self.days_of_week

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

    def get_tour_model_from(self, place):
        return _dfs(self, place)

    def __repr__(self):
        return self.get_tour_model_from(home)

    def __str__(self):
        return self.get_tour_model_from(home)

    def win_state(self):
        return _bfs(self)
        

def _dfs(TM, starting_point):
    s = [ ]
    came_from = { }
    s.append(starting_point)
    curr = None
    while len(s) > 0:
        node = s.pop()
        node.visit()
        TM.all_locs.add(node)
        #print "visited %s" % node
        #print "%s : %s" % (node, node.times_allowed_one)
        successors = node.get_successors()
        #print successors
        for v in successors:
            if v.times_allowed_one > 0:
                #print v
                s.append(v)
                #print "s is %s" % s
                came_from["%s%s" % (v, v.times_allowed_one)] = node
                print "came_from[%s%s] = %s" % (v, v.times_allowed_one, node)
                curr = v
    print came_from
    for node in TM.all_locs:
        print "%s : %s" % (node, node.count)
        node.count_down = node.count

    return _make_path(came_from, curr, starting_point)

def _make_path(came_from, currNode, starting_point):
    print "%s%s" % (currNode, currNode.count_down)
    print "%s%s" % (currNode, currNode.times_allowed_two)
    if ( ("%s%s" % (currNode, currNode.count_down) ) in came_from) and (currNode.times_allowed_two > 0):
        print "%s : %s" % (currNode, currNode.count_down)
        #print "add this %s : %s" % (currNode, came_from[currNode])
        currNode.count_down -= 1
        currNode.times_allowed_two -= 1

        temp = _make_path(came_from, came_from["%s%s" % (currNode, currNode.count_down+1)], starting_point)


        return temp + [currNode]
    else:
        return [starting_point]



def _bfs(TM):
    q = Queue.queue
    visited = set( )
    start = TM.home
    visited.add(start)
    q.push(start)
    while not q.empty():
        t = q.pop()
        successors = t.get_successors()
        for v in successors:
            if v not in visited:
                if v.num_times_allowed_one > 0:
                    return False
                visited.add(v)
                q.push(v)
    return True


def test_something():
    """ Basic Sanity Check """
    home = Location("home")
    work = Location("work")
    coffee = Location("coffee")
    home.is_home = True
    home.add_to_successors(work)
    work.add_to_successors(coffee)
    tm = TourModel(home, "test")
    tm_from_home = tm.get_tour_model_from(home)
    print tm_from_home


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
    print tm.get_tour_model_from(work)


def test_case_two():
    """ Work to coffee to work to lunch and then back to work; slightly more complex """
    home = Location("home")
    work = Location("work")
    coffee = Location("coffee")
    lunch = Location("lunch")
    home.add_to_successors(work)
    # work.times_allowed_one -= 1
    # work.times_allowed_two -= 1
    work.add_to_successors(coffee)
    coffee.add_to_successors(work)
    work.add_to_successors(lunch)
    lunch.add_to_successors(work)
    tm = TourModel(home, "work -> coffee -> work -> lunch -> work")
    print "returning %s" % tm.get_tour_model_from(work)





