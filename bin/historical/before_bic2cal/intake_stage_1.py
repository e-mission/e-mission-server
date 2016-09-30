import json
import logging

import emission.analysis.intake.cleaning.clean_and_resample as eaicr
import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.net.usercache.abstract_usercache_handler as euah
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.aggregate_timeseries as estag
import net.ext_service.habitica.executor as autocheck

if __name__ == '__main__':
    try:
        intake_log_config = json.load(open("conf/log/intake.conf", "r"))
    except:
        intake_log_config = json.load(open("conf/log/intake.conf.sample", "r"))

    intake_log_config["handlers"]["file"]["filename"] = intake_log_config["handlers"]["file"]["filename"].replace("intake", "intake_1")
    intake_log_config["handlers"]["errors"]["filename"] = intake_log_config["handlers"]["errors"]["filename"].replace("intake", "intake_1")

    logging.config.dictConfig(intake_log_config)

    all_long_term_uuid_list = esta.TimeSeries.get_uuid_list()

    # TEST_PHONE_IDS are not critical - we can run a pipeline for them once a day
    filtered_long_term_uuid_list = [u for u in all_long_term_uuid_list if u not in estag.TEST_PHONE_IDS]
    half = len(filtered_long_term_uuid_list)/2
    long_term_uuid_list = filtered_long_term_uuid_list[:half]

    logging.info("*" * 10 + "long term UUID list = %s" % long_term_uuid_list)
    for uuid in long_term_uuid_list:
        if uuid is None:
            continue

        logging.info("*" * 10 + "UUID %s: filter accuracy if needed" % uuid + "*" * 10)
        eaicf.filter_accuracy(uuid)
        
        logging.info("*" * 10 + "UUID %s: segmenting into trips" % uuid + "*" * 10)
        eaist.segment_current_trips(uuid)

        logging.info("*" * 10 + "UUID %s: segmenting into sections" % uuid + "*" * 10)
        eaiss.segment_current_sections(uuid)

        logging.info("*" * 10 + "UUID %s: smoothing sections" % uuid + "*" * 10)
        eaicl.filter_current_sections(uuid)

        logging.info("*" * 10 + "UUID %s: cleaning and resampling timeline" % uuid + "*" * 10)
        eaicr.clean_and_resample(uuid)

        logging.info("*" * 10 + "UUID %s: checking active mode trips to autocheck habits" % uuid + "*" * 10)
        autocheck.give_points_for_all_tasks(uuid)

        logging.info("*" * 10 + "UUID %s: storing views to cache" % uuid + "*" * 10)
        uh = euah.UserCacheHandler.getUserCacheHandler(uuid)
        uh.storeViewsToCache()
