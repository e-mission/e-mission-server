import json
import logging

import emission.net.usercache.abstract_usercache_handler as euah
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.aggregate_timeseries as estag
import emission.storage.decorations.tour_model_queries as esdtmq

import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.intake.cleaning.clean_and_resample as eaicr
import emission.net.ext_service.habitica.sync_habitica as autocheck

if __name__ == '__main__':
    try:
        usercache_log_config = json.load(open("conf/log/intake.conf", "r"))
    except:
        usercache_log_config = json.load(open("conf/log/intake.conf.sample", "r"))

    logging.config.dictConfig(usercache_log_config)

    cache_uuid_list = enua.UserCache.get_uuid_list()
    logging.info("cache UUID list = %s" % cache_uuid_list)

    for uuid in cache_uuid_list:
        logging.info("*" * 10 + "UUID %s: moving to long term" % uuid + "*" * 10)
        uh = euah.UserCacheHandler.getUserCacheHandler(uuid)
        uh.moveToLongTerm()
