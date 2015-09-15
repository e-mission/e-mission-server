import logging
import attrdict as ad

import emission.net.usercache.abstract_usercache_handler as enuah
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as etsa

import emission.net.usercache.formatters.formatter as enuf
import emission.storage.pipeline_queries as esp
import emission.core.get_database as edb

class BuiltinUserCacheHandler(enuah.UserCacheHandler):
    def __init__(self, uuid):
        super(BuiltinUserCacheHandler, self).__init__(uuid)
       
    def moveToLongTerm(self):
        """
        In order to move to the long term, we need to do the following:
        a) determine the time range to be processed. We do this by checking the
            pipeline state. this does not leak information since the process
            will run whether there is data for it to work on or not. So the
            pipeline state is stored outside of the user cache.
        b) process the time range. pass in a function that works on every entry
            to convert it to the appropriate format.
        c) delete the time range once it is processed (in usercache or here?)
        d) update the pipeline state to reflect the new range (here)
        """
        # Error handling: if any of the entries has an error in processing, we
        # move it to a separate "error_usercache" and process the rest. The
        # stage is still marked successful. This means that the stage can never
        # be unsuccessful. We could try to keep it, but then the delete query
        # below will get significantly more complicated.
        time_query = esp.get_time_range_for_usercache(self.uuid)
        uc = enua.UserCache.getUserCache(self.uuid)
        ts = etsa.TimeSeries.get_time_series(self.uuid)

        curr_entry_it = uc.getMessage(time_query)
        for entry_doc in curr_entry_it:
            unified_entry = None
            try:
                # We don't want to use our wrapper classes yet because they are based on the
                # standard long-term formats, and we don't yet know whether the
                # incoming entries are consistent with them. That's why we have the
                # convert_to_common_format step. So let's just wrap this in a
                # generic attrdict for now.
                entry = ad.AttrDict(entry_doc)
                unified_entry = enuf.convert_to_common_format(entry)
                ts.insert(unified_entry)
            except Exception as e:
                logging.exception("Backtrace time")
                logging.warn("Got error %s while saving entry %s -> %s"% (e, entry, unified_entry))
                ts.insert_error(entry_doc)
        uc.clearProcessedMessages(time_query)
        esp.mark_usercache_done(self.uuid)
