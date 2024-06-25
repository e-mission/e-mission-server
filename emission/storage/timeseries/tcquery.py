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
    Object that encapsulates a query for filtering based on localdate objects.
    This works as a set of filters for each localdate field, e.g. year, month, day, etc.
    Useful for filtering on one or more localdate fields
    e.g. TimeComponentQuery("data.start_local_dt", {"weekday": 0}, {"weekday": 4})
    For range queries, use FmtTimeQuery instead.
    """
    def __init__(self, timeType, startLD, endLD):
        self.timeType = timeType
        self.startLD = startLD
        self.endLD = endLD

    def get_query(self):
        return esdl.get_filter_query(self.timeType, self.startLD, self.endLD)
