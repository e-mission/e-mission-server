from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import json
import logging
import argparse

import emission.net.ext_service.push.notify_usage as pnu

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(prog="silent_ios_push")
    parser.add_argument("interval",
        help="specify the sync interval that the phones have subscribed to",
        type=int)
    parser.add_argument("-d", "--dev", action="store_true", default=False)

    args = parser.parse_args()
    logging.debug("About to send notification to phones with interval %d" % args.interval)
    response = pnu.send_silent_notification_to_ios_with_interval(args.interval, dev=args.dev)
    pnu.display_response(response)
