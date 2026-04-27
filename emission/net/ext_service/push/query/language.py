# Input spec sample at
# emission/net/ext_service/push/sample.specs/platform.query.sample

# Input: query spec
# Output: list of uuids
# 

from builtins import *
import logging

import emission.net.ext_service.push.notify_queries as pnq

def query(spec):
  userid_list = pnq.get_matching_user_ids(spec)
  return userid_list
