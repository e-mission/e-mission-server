from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
from builtins import object
import optparse
import sys
import json
import logging

# Our imports
import emission.analysis.modelling.user_model.alternative_trips_module as eatm
import emission.storage.timeseries.abstract_timeseries as esta

class AlternativeTripsPipeline(object):
    def __init__(self):
        pass

    def get_trips_for_alternatives(self, user_uuid):
        pass

    def runPipeline(self, immediate=False):
        for user_uuid in esta.TimeSeries.get_uuid_list():
            logging.debug("Finding Trips for User: %s" % user_uuid)
            trips_with_no_alternatives = self.get_trips_for_alternatives(user_uuid)
            eatm.calc_alternative_trips(trips_with_no_alternatives, immediate)

def commandArgs(argv):
    parser = optparse.OptionParser(description = '')
    parser.add_option('--immediate',
                      dest = 'immediate',
                      default = False,
                      help = 'Find Alternatives NOW')
    (options, args) = parser.parse_args(argv)
    if options.immediate:
        return True

if __name__ == "__main__":
    immediate = commandArgs(sys.argv)
    with open('config.json') as cf
        config_data = json.load(cf)
    log_base_dir = config_data['paths']['log_base_dir']
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                        filename="%s/pipeline.log" % log_base_dir, level=logging.DEBUG)
    alternativesPipeline = AlternativeTripsPipeline()
    alternativesPipeline.runPipeline(immediate)



