import argparse
import sys
import logging
import uuid

import emission.core.get_database as edb
import emission.core.wrapper.user as ecwu
import emission.net.ext_service.habitica.proxy as proxy

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-e", "--user_email")
    group.add_argument("-u", "--user_uuid")

    args = parser.parse_args()

    if args.user_uuid:
        del_uuid = uuid.UUID(args.user_uuid)
    else:
        del_uuid = ecwu.User.fromEmail(args.user_email).uuid

    logging.debug("Found uuid %s" % del_uuid)
    del_habitica_creds = edb.get_habitica_db().find_one({'user_id': del_uuid})
    logging.debug("del_habitica_creds = %s" % del_habitica_creds)
    del_result = proxy.habiticaProxy(del_uuid, "DELETE",
                                     "/api/v3/user",
                                     {'password': del_habitica_creds['habitica_password']})
    logging.debug("delete result = %s" % del_result)
    edb.get_habitica_db().remove({'user_id': del_uuid})
