import argparse
import sys
import logging

import emission.core.get_database as edb
import emission.net.ext_service.habitica.proxy as proxy

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("user_email",
        help="the email address of the user whose habitica account you want to clean up")

    args = parser.parse_args()
    del_uuid = edb.get_uuid_db().find_one({'user_email': args.user_email})['uuid']
    logging.debug("Found uuid %s" % del_uuid)
    del_habitica_creds = edb.get_habitica_db().find_one({'user_id': del_uuid})
    logging.debug("del_habitica_creds = %s" % del_habitica_creds)
    del_result = proxy.habiticaProxy(del_uuid, "DELETE",
                                     "/api/v3/user",
                                     {'password': del_habitica_creds['habitica_password']})
    logging.debug("delete result = %s" % del_result)
