from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import arrow
import emission.core.wrapper.wrapperbase as ecwb

class LocalDate(ecwb.WrapperBase):
    """
    Supporting wrapper class that stores the expansions of the components
    of a datetime in *the local timezone* to allow us to query heatmaps based
    on local time. This is a recurring pattern, so we create a supporting
    datastructure then we can index the entire document instead of indexing
    each individual field.
    """
    props = {
                "year": ecwb.WrapperBase.Access.WORM,
                "month": ecwb.WrapperBase.Access.WORM,
                "day": ecwb.WrapperBase.Access.WORM,
                "hour": ecwb.WrapperBase.Access.WORM,
                "minute": ecwb.WrapperBase.Access.WORM,
                "second": ecwb.WrapperBase.Access.WORM,
                "weekday": ecwb.WrapperBase.Access.WORM,
                "timezone": ecwb.WrapperBase.Access.WORM
            }

    enums = {}
    geojson = []
    nullable = [] 
    local_dates = []

    def _populateDependencies(self):
        pass

    @staticmethod
    def get_local_date(ts, timezone):
        adt = arrow.get(ts).to(timezone)
        return LocalDate({'year': adt.year, 'month': adt.month, 'day': adt.day,
        'hour': adt.hour, 'minute': adt.minute, 'second': adt.second,
        'weekday': adt.weekday(), 'timezone': timezone})

