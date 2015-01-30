import logging

import sys
import os

sys.path.append("%s" % os.getcwd())
sys.path.append("%s/../CFC_WebApp/" % os.getcwd())

from get_database import get_uuid_db
from main import userclient

class PrecomputeResults:
    def __init__(self):
        pass

    def precomputeResults(self):
        # TODO: Ensure that the default client is "default", which will make
        # life much easier overall
        for user_uuid_dict in get_uuid_db().find({}, {'uuid': 1, '_id': 0}):
            logging.info("Computing precomputed results for %s" % user_uuid_dict['uuid'])
            userclient.runClientSpecificBackgroundTasks(user_uuid_dict['uuid'])

if __name__ == '__main__':
    import json

    config_data = json.load(open('config.json'))
    log_base_dir = config_data['paths']['log_base_dir']
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                        filename="%s/precompute_results.log" % log_base_dir, level=logging.DEBUG)

    pr = PrecomputeResults()
    pr.precomputeResults()
