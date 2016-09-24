import sys
import json
import logging
import numpy as np
import arrow

import emission.core.get_database as edb

import emission.net.usercache.abstract_usercache_handler as euah
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.aggregate_timeseries as estag

import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.intake.cleaning.clean_and_resample as eaicr
import emission.net.ext_service.habitica.sync_habitica as autocheck


def run_intake_pipeline(process_number, uuid_list):
    """
    Run the intake pipeline with the specified process number and uuid list.
    Note that the process_number is only really used to customize the log file name
    We could avoid passing it in by using the process id - os.getpid() instead, but
    then we won't get the nice RotatingLogHandler properties such as auto-deleting
    files if there are too many. Maybe it will work properly with logrotate? Need to check
    :param process_number: id representing the process number. In range (0..n)
    :param uuid_list: the list of UUIDs that this process will handle
    :return:
    """
    try:
        intake_log_config = json.load(open("conf/log/intake.conf", "r"))
    except:
        intake_log_config = json.load(open("conf/log/intake.conf.sample", "r"))

    intake_log_config["handlers"]["file"]["filename"] = \
        intake_log_config["handlers"]["file"]["filename"].replace("intake", "intake_%s" % process_number)
    intake_log_config["handlers"]["errors"]["filename"] = \
        intake_log_config["handlers"]["errors"]["filename"].replace("intake", "intake_%s" % process_number)

    logging.config.dictConfig(intake_log_config)
    # np.random.seed(61297777)

    logging.info("processing UUID list = %s" % uuid_list)

    for uuid in uuid_list:
        if uuid is None:
            continue

        # Hack until we delete these spurious entries
        # https://github.com/e-mission/e-mission-server/issues/407#issuecomment-2484868

        if edb.get_timeseries_db().find({"user_id": uuid}).count() == 0:
            logging.debug("Found no entries for %s, skipping")
            continue

        uh = euah.UserCacheHandler.getUserCacheHandler(uuid)

        logging.info("*" * 10 + "UUID %s: moving to long term" % uuid + "*" * 10)
        print(str(arrow.now()) + "*" * 10 + "UUID %s: moving to long term" % uuid + "*" * 10)

        uh.moveToLongTerm()


        logging.info("*" * 10 + "UUID %s: filter accuracy if needed" % uuid + "*" * 10)
        print(str(arrow.now()) + "*" * 10 + "UUID %s: filter accuracy if needed" % uuid + "*" * 10)
        eaicf.filter_accuracy(uuid)

        
        logging.info("*" * 10 + "UUID %s: segmenting into trips" % uuid + "*" * 10)
        print(str(arrow.now()) + "*" * 10 + "UUID %s: segmenting into trips" % uuid + "*" * 10)
        eaist.segment_current_trips(uuid)


        logging.info("*" * 10 + "UUID %s: segmenting into sections" % uuid + "*" * 10)
        print(str(arrow.now()) + "*" * 10 + "UUID %s: segmenting into sections" % uuid + "*" * 10)
        eaiss.segment_current_sections(uuid)


        logging.info("*" * 10 + "UUID %s: smoothing sections" % uuid + "*" * 10)
        print(str(arrow.now()) + "*" * 10 + "UUID %s: smoothing sections" % uuid + "*" * 10)
        eaicl.filter_current_sections(uuid)


        logging.info("*" * 10 + "UUID %s: cleaning and resampling timeline" % uuid + "*" * 10)
        print(str(arrow.now()) + "*" * 10 + "UUID %s: cleaning and resampling timeline" % uuid + "*" * 10)
        eaicr.clean_and_resample(uuid)


        logging.info("*" * 10 + "UUID %s: checking active mode trips to autocheck habits" % uuid + "*" * 10)
        print(str(arrow.now()) + "*" * 10 + "UUID %s: checking active mode trips to autocheck habits" % uuid + "*" * 10)
        autocheck.reward_active_transportation(uuid)


        logging.info("*" * 10 + "UUID %s: storing views to cache" % uuid + "*" * 10)
        print(str(arrow.now()) + "*" * 10 + "UUID %s: storing views to cache" % uuid + "*" * 10)
        uh.storeViewsToCache()
