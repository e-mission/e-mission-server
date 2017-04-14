import logging

def analyse_timeline(entries):
    logging.info("Analyzing timeline...")
    logging.info("timeline has %d entries" % len(entries))

    unique_user_list = set(map(lambda e: e["user_id"], entries))
    logging.info("timeline has data from %d users" % len(unique_user_list))
    unique_user_list_list = list(unique_user_list)
    logging.info("Found %d entries with blank uuid, loading them anyway" % 
        (len([entry for entry in entries if entry["user_id"] == ''])))

    unique_key_list = set(map(lambda e: e["metadata"]["key"], entries))
    logging.info("timeline has the following unique keys %s" % unique_key_list)

    if "analysis/cleaned_trip" in unique_key_list and "analysis/cleaned_place" in unique_key_list:
        logging.info("timeline contains analysis results, no need to run the intake pipeline")
    else:
        logging.info("timeline contains only raw data, need to run the intake pipeline")
    return unique_user_list
