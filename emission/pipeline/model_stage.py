import sys
import logging
import emission.net.usercache.abstract_usercache_handler as euah
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.tour_model_queries as esdtmq

import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.intake.cleaning.clean_and_resample as eaicr
import emission.net.ext_service.habitica.executor as autocheck


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                        level=logging.DEBUG)

    long_term_uuid_list = esta.TimeSeries.get_uuid_list()
    logging.info("*" * 10 + "long term UUID list = %s" % long_term_uuid_list)
    for uuid in long_term_uuid_list:
        if uuid is None:
            continue

        logging.info("*" * 10 + "UUID %s: finding common trips" % uuid + "*" * 10)
        esdtmq.make_tour_model_from_raw_user_data(uuid)

