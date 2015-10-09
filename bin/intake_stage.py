import sys
import logging
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
    level=logging.DEBUG)

import emission.net.usercache.abstract_usercache_handler as euah
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta

import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.location_smoothing as eaicl

if __name__ == '__main__':
    cache_uuid_list = enua.UserCache.get_uuid_list()
    logging.info("cache UUID list = %s" % cache_uuid_list)

    for uuid in cache_uuid_list:
        logging.info("UUID %s: moving to long term" % uuid)
        uh = euah.UserCacheHandler.getUserCacheHandler(uuid)
        uh.moveToLongTerm()

    long_term_uuid_list = esta.TimeSeries.get_uuid_list()
    logging.info("long term UUID list = %s" % long_term_uuid_list)
    for uuid in long_term_uuid_list:
        logging.info("UUID %s: segmenting into trips" % uuid)
        eaist.segment_current_trips(uuid)

        logging.info("UUID %s: segmenting into sections" % uuid)
        eaiss.segment_current_sections(uuid)

        logging.info("UUID %s: smoothing sections" % uuid)
        eaicl.filter_current_sections(uuid)

        logging.info("UUID %s: storing views to cache" % uuid)
        uh = euah.UserCacheHandler.getUserCacheHandler(uuid)
        uh.storeViewsToCache()
