import logging

import sys
import os

sys.path.append("%s" % os.getcwd())
sys.path.append("%s/../CFC_WebApp/" % os.getcwd())

from get_database import get_uuid_db
from main import userclient, carbon
from dao.user import User

class PrecomputeResults:
    def __init__(self):
        pass

    # This should really be pulled out into a separate default client
    def precomputeDefault(self, user_uuid):
      user = User.fromUUID(user_uuid)
      # carbon compare results is a tuple. Tuples are converted to arrays
      # by mongodb
      # In [44]: testUser.setScores(('a','b', 'c', 'd'), ('s', 't', 'u', 'v'))
      # In [45]: testUser.getScore()
      # Out[45]: ([u'a', u'b', u'c', u'd'], [u's', u't', u'u', u'v'])
      carbonCompareResults = carbon.getFootprintCompare(user_uuid)
      user.setScores(None, carbonCompareResults)

    def precomputeResults(self):
        for user_uuid_dict in get_uuid_db().find({}, {'uuid': 1, '_id': 0}):
            logging.info("Computing precomputed results for %s" % user_uuid_dict['uuid'])
            userclient.runClientSpecificBackgroundTasks(user_uuid_dict['uuid'], self.precomputeDefault)

if __name__ == '__main__':
    import json

    config_data = json.load(open('config.json'))
    log_base_dir = config_data['paths']['log_base_dir']
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                        filename="%s/precompute_results.log" % log_base_dir, level=logging.DEBUG)

    pr = PrecomputeResults()
    pr.precomputeResults()
