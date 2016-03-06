import sys
import logging
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
    level=logging.DEBUG)

import emission.net.usercache.abstract_usercache_handler as euah
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta

import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.modelling.tour_model.cluster_pipeline as eamtmcp

if __name__ == '__main__':
    cache_uuid_list = enua.UserCache.get_uuid_list()
    logging.info("cache UUID list = %s" % cache_uuid_list)

    for uuid in cache_uuid_list:
        logging.info("*" * 10 + "UUID %s: moving to long term" % uuid + "*" * 10)
        uh = euah.UserCacheHandler.getUserCacheHandler(uuid)
        uh.moveToLongTerm()

    # TODO: For now, move filters from metadata to data. Once we get the
    # updated data collection clients to people, we don't need to do this any
    # more
    import emission.storage.timeseries.format_hacks.move_filter_field as estfm
    estfm.move_all_filters_to_data()

    long_term_uuid_list = esta.TimeSeries.get_uuid_list()
    logging.info("*" * 10 + "long term UUID list = %s" % long_term_uuid_list)
    for uuid in long_term_uuid_list:
        logging.info("*" * 10 + "UUID %s: filter accuracy if needed" % uuid + "*" * 10)
        eaicf.filter_accuracy(uuid)
        
        logging.info("*" * 10 + "UUID %s: segmenting into trips" % uuid + "*" * 10)
        eaist.segment_current_trips(uuid)

        logging.info("*" * 10 + "UUID %s: segmenting into sections" % uuid + "*" * 10)
        eaiss.segment_current_sections(uuid)

        logging.info("*" * 10 + "UUID %s: smoothing sections" % uuid + "*" * 10)
        eaicl.filter_current_sections(uuid)

        logging.info("*" * 10 + "UUID %s: finding common trips" % uuid + "*" * 10)
        eamtmcp.main(uuid, False)

        logging.info("*" * 10 + "UUID %s: storing views to cache" % uuid + "*" * 10)
        uh = euah.UserCacheHandler.getUserCacheHandler(uuid)
        uh.storeViewsToCache()
        uh.storeCommonTripsToCache()


