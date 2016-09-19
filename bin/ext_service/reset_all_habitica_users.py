import argparse
import sys
import logging

import emission.core.get_database as edb
import emission.net.ext_service.habitica.proxy as proxy

def reset_user(reset_em_uuid):
    del_result = proxy.habiticaProxy(reset_em_uuid, "POST",
                                     "/api/v3/user/reset", {})
    logging.debug("reset result for %s = %s" % (reset_em_uuid, del_result))

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    for creds in edb.get_habitica_db().find():
        reset_uuid = creds["user_id"]
        logging.debug("Processing emission user id %s" % reset_uuid)
        reset_user(reset_uuid)

