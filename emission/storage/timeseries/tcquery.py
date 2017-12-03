from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from builtins import object
import emission.net.usercache.abstract_usercache as enua
import emission.storage.decorations.local_date_queries as esdl

class TimeComponentQuery(object):
    """
    Object that encapsulates a query for a particular time at the local time in
    the timezone where the data was generated.
    """
    def __init__(self, timeType, startLD, endLD):
        self.timeType = timeType
        self.startLD = startLD
        self.endLD = endLD

    def get_query(self):
        return esdl.get_range_query(self.timeType, self.startLD, self.endLD)
