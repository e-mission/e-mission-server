# Input spec sample at
# emission/net/ext_service/push/sample.specs/platform.query.sample

# Input: query spec
# Output: list of uuids
# 

import logging

import emission.net.ext_service.push.notify_queries as pnq

def query(spec):
  platform = spec['platform']
  userid_list = pnq.get_matching_user_ids(pnq.get_platform_query(platform))
  return userid_list
