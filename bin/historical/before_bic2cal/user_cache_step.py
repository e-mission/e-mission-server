import json
import logging

import emission.net.usercache.abstract_usercache as enua
import emission.net.usercache.abstract_usercache_handler as euah

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
