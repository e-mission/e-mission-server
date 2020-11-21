from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Input spec sample at
# emission/net/ext_service/push/sample.specs/platform.query.sample

# Input: query spec
# Output: list of uuids
# 

from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

import emission.net.ext_service.push.notify_queries as pnq

def query(spec):
  userid_list = pnq.get_matching_user_ids(spec)
  return userid_list
