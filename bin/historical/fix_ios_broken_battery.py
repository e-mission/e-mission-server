from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import emission.core.get_database as edb

edb.get_timeseries_db().remove({'metadata.key': 'background/battery',
                                'data.battery_level_pct': -100})
