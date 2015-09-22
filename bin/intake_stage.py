import sys
import logging
import emission.net.usercache.abstract_usercache_handler as euah
import emission.net.usercache.abstract_usercache as enua

import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    uuid_list = enua.UserCache.get_uuid_list()
    print("UUID list = %s" % uuid_list)
    for uuid in uuid_list:
        print("Processing uuid %s" % uuid)
        uh = euah.UserCacheHandler.getUserCacheHandler(uuid)
        uh.moveToLongTerm()
        eaist.segment_current_trips(uuid)
        eaiss.segment_current_sections(uuid)
