from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

import emission.storage.pipeline_queries as esp

def get_complete_ts(user_id):
    complete_ts = esp.get_complete_ts(user_id)
    logging.debug("Returning complete_ts = %s" % complete_ts)
    return complete_ts
