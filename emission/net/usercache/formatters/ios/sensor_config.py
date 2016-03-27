import logging

import pytz

# Currently, we just reflect this back to the user, so not much editing to do
# here. Since we get the timezone from javascript guessing, though, let's just
# verify that it is correct.
def format(entry):
    formatted_entry = entry
    try:
        valid_tz = pytz.timezone(entry.metadata.time_zone)
    catch UnknownTimeZoneError, e:
        logging.warn("Got error %s while checking format validity" % e)
        # Default timezone in for the Bay Area, which is probably a fairly safe
        # assumption for now
        formatted_entry.metadata.time_zone = "America/Los_Angeles"
    return formatted_entry;
