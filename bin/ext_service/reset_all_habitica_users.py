import argparse
import sys
import logging

import emission.core.get_database as edb
import emission.net.ext_service.habitica.proxy as proxy
import emission.net.ext_service.habitica.sync_habitica as autocheck

def reset_user(reset_em_uuid):
    del_result = proxy.habiticaProxy(reset_em_uuid, "POST",
                                     "/api/v3/user/reset", {})
    update_result = edb.get_habitica_db().update({"user_id": reset_em_uuid},
                                 {"$set": {'metrics_data':
                                   {'last_timestamp': 0, 'bike_count': 0, 'walk_count': 0}}})
    logging.debug("reset result for %s = %s, %s" % (reset_em_uuid, del_result, update_result))

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--restore",
                        help="re-run pipeline and restore values", action="store_true")
    args = parser.parse_args()

    for creds in edb.get_habitica_db().find():
        reset_uuid = creds["user_id"]
        logging.debug("Processing emission user id %s" % reset_uuid)
        reset_user(reset_uuid)
        if args.restore:
            autocheck.reward_active_transportation(reset_uuid)


