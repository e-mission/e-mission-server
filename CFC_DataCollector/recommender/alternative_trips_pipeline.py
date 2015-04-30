import optparse
import sys
import json
from alternative_trips_module import calc_alternative_trips
import tripiterator as ti
from common import get_uuid_list, uuid_to_name 
import logging

from trip import E_Mission_Trip

class AlternativeTripsPipeline:
    def __init__(self):
        pass

    def get_trips_for_alternatives(self, user_uuid):
        return ti.TripIterator(user_uuid, ["trips", "get_all"])

    def runPipeline(self, immediate):
        for user_uuid in get_uuid_list():
	    print "Finding Trips for User: ", uuid_to_name(str(user_uuid)), user_uuid
            trips_with_no_alternatives = self.get_trips_for_alternatives(user_uuid)
            calc_alternative_trips(trips_with_no_alternatives, immediate)

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
    config_data = json.load(open('config.json'))
    log_base_dir = config_data['paths']['log_base_dir']
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                        filename="%s/pipeline.log" % log_base_dir, level=logging.DEBUG)
    alternativesPipeline = AlternativeTripsPipeline()
    alternativesPipeline.runPipeline(immediate)



