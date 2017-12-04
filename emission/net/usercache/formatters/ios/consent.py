from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import copy
import pytz

import emission.core.wrapper.consentconfig as ecws
import emission.net.usercache.formatters.common as fc

# Currently, we just reflect this back to the user, so not much editing to do
# here. Since we get the timezone from javascript guessing, though, let's just
# verify that it is correct.
def format(entry):
    formatted_entry = entry

    metadata = entry.metadata
    try:
        valid_tz = pytz.timezone(entry.metadata.time_zone)
    except pytz.UnknownTimeZoneError as e:
        logging.warning("Got error %s while checking format validity" % e)
        # Default timezone in for the Bay Area, which is probably a fairly safe
        # assumption for now
        metadata.time_zone = "America/Los_Angeles"
    # adds the python datetime and fmt_time entries. important for future searches!
    fc.expand_metadata_times(metadata)
    formatted_entry.metadata = metadata

    formatted_entry.data = entry.data

    return formatted_entry;
