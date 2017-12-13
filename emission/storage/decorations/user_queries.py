from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Utility class to return useful user queries
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

import emission.core.get_database as edb
import emission.storage.timeseries.aggregate_timeseries as estag
from uuid import UUID

def get_all_uuids():
    all_uuids = [e["uuid"] for e in edb.get_uuid_db().find()]
    return all_uuids

