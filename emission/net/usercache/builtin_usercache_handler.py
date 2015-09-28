import logging
import attrdict as ad

import emission.net.usercache.abstract_usercache_handler as enuah
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as etsa

import emission.analysis.plotting.geojson.geojson_feature_converter as gfc

import emission.net.usercache.formatters.formatter as enuf
import emission.storage.pipeline_queries as esp
import emission.core.get_database as edb

class BuiltinUserCacheHandler(enuah.UserCacheHandler):
    def __init__(self, user_id):
        super(BuiltinUserCacheHandler, self).__init__(user_id)
       
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
        time_query = esp.get_time_range_for_usercache(self.user_id)
        uc = enua.UserCache.getUserCache(self.user_id)
        ts = etsa.TimeSeries.get_time_series(self.user_id)

        curr_entry_it = uc.getMessage(None, time_query)
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
        esp.mark_usercache_done(self.user_id)

    def storeViewsToCache(self):
        """
        Determine which "documents" need to be sent to the usercache. This
        is currently the trips for the past three days. Any entries older than 3 days
        should be purged. Note that this currently repeats information - the data that
        was from day before yesterday, for example, would have been sent at that point
        as well.  As an optimization, we could use something like CouchDB to only send
        the diffs back and forth. Or use a simple syncing mechanism in which the
        write_ts of the "document" reflects time that the documents were generated, and
        only sync new documents? In general, the write_ts of the generated document
        should be within a few hours of the intake document.

        Second question: How do we send back the travel diary data? As the
        raw entries from the database, or in geojson format?

        After much thought, we are planning to send materialized views over
        the data in various json formats. In particular, we plan to send
        the trip information in geojson format, and the result
        visualizations in either vega or mpld3 formats.

        Since we plan to store all the data in a giant event database,
        materializing the views is likely to be a complex process. Doing
        the materialization ahead of time helps with responsiveness. Note
        that in principle, we could store the timeseries and materialize
        views directly on the phone as well, but in general we choose to do that on the
        server for flexibility and ease of programming.

        This also means that the write_ts of the view naturally corresponds
        to the time that it was generated, as opposed to sensed, which
        provides us with all kinds of goodness.

        :return: Nothing. As a side effect, we materialize views from the
        data and store them into the usercache to be sent to the phone
        """
        # Currently the only views generated are the geojson
        # representations of the trips for the last 3 days. By default, the
        # write_ts of the entry is the time that it was generated, which
        # may not be the right choice for queries on the phone. There, we
        # want to query by start or end time of the trip.
        #
        # Right now, we will go with generated time, since it is unlikely
        # to be off by a lot from the end time, at least good enough for searching in a day.
        # need to decide whether to provide customizations to search by
        # data in addition to metadata.

        # Finally, we don't really want to generate data for trips that we
        # haven't finished analysis for. Right now, we only perform two
        # kinds of analyses - trip and section segmentation, but later we
        # may perform both mode and semantic analyses. We only want to send
        # the data to the phone once analyses are complete. In particular,
        # we don't want to send trips for which we haven't yet generated
        # sections.

        # We could create a new pipeline state for this. But instead, we
        # just query starting from the last "done" ts of the last pipeline
        # stage instead of "now". The last "done" ts is the start_ts if the
        # pipeline were to run again

        start_ts = esp.get_complete_ts(self.user_id)
        seventy_two_hours_ago_ts = start_ts - 60 * 24 * 60 * 60 # 3 days in seconds

        trip_gj_list = gfc.get_geojson_for_range(self.user_id, seventy_two_hours_ago_ts, start_ts)
        uc = enua.UserCache.getUserCache(self.user_id)

        uc.putDocument("diary/trips", trip_gj_list)
