import sys
import logging
# Change this to INFO if you want less verbose logging
logging.basicConfig(level=logging.DEBUG)
import argparse
import json

import emission.core.wrapper.suggestion_sys as sugg

def test_find_destination_business(params):
    lat, lon = sugg.geojson_to_lat_lon_separated(params["loc"])
    result = sugg.find_destination_business(lat, lon)
    return list(result)

def test_category_of_business_nominatim(params):
    lat, lon = sugg.geojson_to_lat_lon_separated(params["loc"])
    result = sugg.category_of_business_nominatim(lat, lon)
    return result

def test_calculate_yelp_server_suggestion_for_locations(params):
    start_loc = params["start_loc"]
    end_loc = params["end_loc"]
    distance_in_miles = sugg.distance(sugg.geojson_to_latlon(start_loc), sugg.geojson_to_latlon(end_loc))
    distance_in_meters = distance_in_miles / 0.000621371
    logging.debug("distance in meters = %s" % distance_in_meters)
    # calculation function expects distance in meters
    result = sugg.calculate_yelp_server_suggestion_for_locations(start_loc, end_loc, distance_in_meters)
    return result.get('businessid', None)

def test_single_instance(test_fn, instance):
    logging.debug("-----" + instance["test_name"] + "------")
    param = instance["input"]
    result = test_fn(param)
    exp_output = instance["output"]
    if exp_output == result:
        return True
    else:
        logging.debug("Test %s failed, output = %s, expected %s "
            % (instance["test_name"], result, exp_output))
        return False

# Note: this has to be here because it needs to be after the
# wrapper function is defined but before we use the keys as valid choices while
# setting up the parser

TEST_WRAPPER_MAP = {
    "find_destination_business": test_find_destination_business,
    "category_of_business_nominatim": test_category_of_business_nominatim,
    "calculate_yelp_server_suggestion_for_locations": test_calculate_yelp_server_suggestion_for_locations
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
#     parser.add_argument("-d", "--debug", type=int,
#         help="set log level to DEBUG")
    parser.add_argument("algorithm",
        choices=TEST_WRAPPER_MAP.keys(),
        help="the algorithm to test")
    parser.add_argument("-f", "--infile",
        help="the file that has the inputs and expected outputs. default is emission/integrationTests/suggestionsys/{algorithm}.dataset.json")

    args = parser.parse_args()
#     if args.debug:
#         logging.basicConfig(level=logging.DEBUG)
#     else:
#         logging.basicConfig(level=logging.INFO)

    if args.infile is None:
        args.infile = ("emission/integrationTests/suggestionsys/%s.dataset.json"
            % (args.algorithm))

    test_fn = TEST_WRAPPER_MAP[args.algorithm]
    logging.info("Mapped algorithm %s -> %s" % (args.algorithm, test_fn))

    dataset = json.load(open(args.infile))
    successfulTests = 0
    failedTests = 0
    for instance in dataset:
        result = test_single_instance(test_fn, instance)
        if result:
            successfulTests = successfulTests + 1
        else:
            failedTests = failedTests + 1
        logging.debug("After instance %s, successfulTests = %d, failedTests = %d"
            % (instance["test_name"], successfulTests, failedTests))

    logging.info("Test complete, overall accuracy = %s " % ((successfulTests * 100) / (successfulTests + failedTests)))

