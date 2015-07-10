class Location(object):

    def __init__(self, name):
        self.successors = [ ]
        self.days_of_week = [ ]
        self.name = name
        self.times_allowed_one = 0
        self.times_allowed_two = 0
        self.count = 0
        self.count_down = 0

    def add_to_successors(self, loc):
        self.successors.append(loc)
        loc.times_allowed_one += 1
        loc.times_allowed_two += 1
        loc.count += 1

    def get_successors(self):
        return self.successors

    def visit(self):
        self.times_allowed_one -= 1

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

    def __eq__(self, other):
        return self.name == other.name


class TourModel(object):

    def __init__(self, home, name):
        self.home = home
        self.name = name
        self.all_locs = set( )
        for loc in self.home.get_successors():
            loc.count -= 1

    def get_tour_model_from(self, place):
        return _dfs(self, place)

    def get_all_tour_models(self):
        tour_models = []
        for place in self.home.get_successors():
            tour_models.append(self.get_tour_model_from(place))
        return tour_models
        

def _dfs(TM, starting_point):
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
