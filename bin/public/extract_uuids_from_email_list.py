from __future__ import print_function
# Exports all data for the particular user for the particular day
# Used for debugging issues with trip and section generation 
import sys
import logging
logging.basicConfig(level=logging.DEBUG)
import gzip
import json

import emission.core.wrapper.user as ecwu

# Input data using one email per line (easy to copy/paste)
# Output data using json (easy to serialize and re-read)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: %s user_email_file user_id_file")
    else:
        user_email_filename = sys.argv[1]
        uuid_filename = sys.argv[2]

        emails = open(user_email_filename).readlines()
        uuids = []
        for e in emails:
            user = ecwu.User.fromEmail(e.strip())
            if user is None:
                logging.warning("Found no mapping for email %s" % e)
            else:
                uuid = user.uuid
                logging.debug("Mapped email %s to uuid %s" % (e, uuid))
                uuids.append(uuid)
                
        uuid_strs = [str(u) for u in uuids]
        json.dump(uuid_strs, open(uuid_filename, "w"))
