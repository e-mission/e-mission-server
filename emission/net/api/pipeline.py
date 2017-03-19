import logging

import emission.storage.pipeline_queries as esp

def get_complete_ts(user_id):
    complete_ts = esp.get_complete_ts(user_id)
    logging.debug("Returning complete_ts = %s" % complete_ts)
    return complete_ts
