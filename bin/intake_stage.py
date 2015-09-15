import sys
import logging
import emission.net.usercache.abstract_usercache_handler as euah
import emission.storage.pipeline_queries as esp

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    uuid_list = esp.get_uuid_list()
    print("UUID list = %s" % uuid_list)
    for uuid in uuid_list:
        print("Processing uuid %s" % uuid)
        uh = euah.UserCacheHandler.getUserCacheHandler(uuid)
        uh.moveToLongTerm()
