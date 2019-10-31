from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
# Converts user emails -> UUIDs
# The UUIDs can be used to extract data for moving across servers
# Typically used as the file input to the
# extract_timeline_for_day_range_and_user.py script
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import *
import sys
import logging
import gzip
import json
import argparse
import bson.json_util as bju

import emission.core.wrapper.user as ecwu

# Input data using one email per line (easy to copy/paste)
# Output data using json (easy to serialize and re-read)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(prog="extract_uuids_from_email_list")
    parser.add_argument("user_email_file")
    parser.add_argument("-o", "--outfile", help="the output filename (default: stdout)")

    args = parser.parse_args()

    user_email_filename = args.user_email_file
    out_fd = sys.stdout if args.outfile is None else open(args.outfile, "w")

    emails = open(user_email_filename).readlines()
    uuids = []
    for e in emails:
        user = ecwu.User.fromEmail(e.strip())
        if user is None:
            logging.warning("Found no mapping for email %s" % e)
        else:
            uuid = user.uuid
            logging.debug("Mapped email %s to uuid %s" % (e.strip(), uuid))
            uuids.append(uuid)

    uuid_strs = [{"uuid": u} for u in uuids]
    json.dump(uuid_strs, out_fd, default=bju.default)
