from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import dateutil.parser as dup
import json
import datetime as pydt
import time

# Our imports
import emission.core.get_database as edb
import emission.net.usercache.abstract_usercache as enua
import emission.analysis.plotting.geojson.geojson_feature_converter as gfc
import emission.core.wrapper.localdate as ecwl

def get_trips_for_day(user_uuid, day, force_refresh):
    """
    The day argument here is a string such as 2015-10-01 or 2016-01-01. We will
    parse this to a datetime, which we will use to query the data in the
    timeseries. We could also cache the timeline views in a separate collection
    and just look up from there. The challenge is to then decide when to
    recompute a view - we can't use the standard technique that we use for the
    other stages because we will have to recompute the timeline for the current
    day multiple times, for example.
    """
    # I was going to read from the user cache if it existed there, and recreate
    # from scratch if it didn't. But that would involve adding a getDocument
    # field to the usercache, which I had intentionally not added before this.
    # The problem with adding a getDocument method is that then the usercache
    # is no longer a cache - it is "storage" that is used internally. If we
    # want to do that, we should really store it as a materialized view and not
    # only in the usercache, which should be a cache of values stored elsewhere.
    parsed_dt = dup.parse(day)
    start_dt = ecwl.LocalDate({'year': parsed_dt.year, 'month': parsed_dt.month, 'day': parsed_dt.day})
    end_dt = start_dt
    return gfc.get_geojson_for_dt(user_uuid, start_dt, end_dt)
