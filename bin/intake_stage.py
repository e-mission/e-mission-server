import sys
import logging
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
    level=logging.DEBUG)

import emission.net.usercache.abstract_usercache_handler as euah
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.tour_model_queries as esdtmq

import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.intake.cleaning.clean_and_resample as eaicr
import emission.net.ext_service.habitica.sync_habitica as autocheck


if __name__ == '__main__':
    cache_uuid_list = enua.UserCache.get_uuid_list()
    logging.info("cache UUID list = %s" % cache_uuid_list)

    for uuid in cache_uuid_list:
        logging.info("*" * 10 + "UUID %s: moving to long term" % uuid + "*" * 10)
        uh = euah.UserCacheHandler.getUserCacheHandler(uuid)
        uh.moveToLongTerm()

    long_term_uuid_list = esta.TimeSeries.get_uuid_list()
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

        logging.info("*" * 10 + "UUID %s: finding common trips" % uuid + "*" * 10)
        esdtmq.make_tour_model_from_raw_user_data(uuid)

        logging.info("*" * 10 + "UUID %s: checking active mode trips to autocheck habits" % uuid + "*" * 10)
        autocheck.auto_complete_tasks(uuid)

        logging.info("*" * 10 + "UUID %s: storing views to cache" % uuid + "*" * 10)
        uh = euah.UserCacheHandler.getUserCacheHandler(uuid)
        uh.storeViewsToCache()
