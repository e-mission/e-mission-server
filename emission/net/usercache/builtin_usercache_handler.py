from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import *
import logging
import attrdict as ad
# This is only to allow us to catch the DuplicateKeyError
import pymongo
import datetime as pydt
import arrow

import emission.net.usercache.abstract_usercache_handler as enuah
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as etsa

import emission.analysis.plotting.geojson.geojson_feature_converter as gfc
import emission.analysis.configs.config as eacc

import emission.net.usercache.formatters.formatter as enuf
import emission.storage.pipeline_queries as esp
# import emission.storage.decorations.tour_model_queries as esdtmpq

import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.entry as ecwe


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
        uc = enua.UserCache.getUserCache(self.user_id)
        messages = uc.getMessage()
        # Here, we assume that the user only has data from a single platform.
        # Since this is a temporary hack, this is fine
        if len(messages) == 0:
            logging.debug("No messages to process")
            # Since we didn't get the current time range, there is no current
            # state, so we don't need to mark it as done
            # esp.mark_usercache_done(None)
            return

        time_query = esp.get_time_range_for_usercache(self.user_id)

        ts = etsa.TimeSeries.get_time_series(self.user_id)

        curr_entry_it = uc.getMessage(None, time_query)
        last_ts_processed = None
        unified_entry_list = []
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
                unified_entry_list.append(unified_entry)
                last_ts_processed = ecwe.Entry(unified_entry).metadata.write_ts
                time_query.endTs = last_ts_processed
            except Exception as e:
                # logging.exception("Error while saving entry %s" % entry)
                logging.warning("Got error %s while saving entry %s -> %s"% (e, entry, unified_entry))
                try:
                    ts.insert_error(entry_doc)
                except pymongo.errors.DuplicateKeyError as e:
                    logging.info("document already present in error timeseries, skipping since read-only")

        if len(unified_entry_list) > 0:
            try:
                ts.bulk_insert(unified_entry_list, etsa.EntryType.DATA_TYPE)
            except pymongo.errors.DuplicateKeyError as e:
                logging.info("document already present in timeseries, skipping since read-only")
        else:
            logging.info("In moveToLongTerm, no entries to save")

        logging.debug("Deleting all entries for query %s" % time_query)
        uc.clearProcessedMessages(time_query)
        esp.mark_usercache_done(self.user_id, last_ts_processed)

    def storeViewsToCache(self):
        """
        Determine which "documents" need to be saved to the usercache.
        """
        time_query = esp.get_time_range_for_output_gen(self.user_id)
        try:
            self.storeTimelineToCache(time_query)
            # self.storeCommonTripsToCache(time_query)
            # last_processed_ts = self.storeConfigsToCache(time_query)
            esp.mark_output_gen_done(self.user_id, last_processed_ts = None)
        except:
            logging.exception("Storing views to cache failed for user %s" % self.user_id)
            esp.mark_output_gen_failed(self.user_id)

    def storeTimelineToCache(self, time_query):
        """
        Store trips for the last week to the cache. Any entries older than 3 days
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
        # haven't finished analysis for. Right now, we only perform three
        # kinds of analyses - trip segmentation, section segmentation and
        # smoothing, but later we may perform both mode and semantic analyses.
        # We only want to send the data to the phone once analyses are
        # complete. In particular, we don't want to send trips for which we
        # haven't yet generated sections.

        # We could create a new pipeline state for this. But instead, we
        # just query starting from the last "done" ts of the last pipeline
        # stage instead of "now". The last "done" ts is the start_ts if the
        # pipeline were to run again

        start_ts = esp.get_complete_ts(self.user_id)
        if start_ts is None:
            logging.info("start_ts = %s, last stage not run successfully, skipping output gen" % start_ts)
            return
        logging.debug("start ts from pipeline = %s, %s" %
           (start_ts, pydt.datetime.utcfromtimestamp(start_ts).isoformat()))
        trip_gj_list = self.get_trip_list_for_seven_days(start_ts)
        if len(trip_gj_list) == 0:
            ts = etsa.TimeSeries.get_time_series(self.user_id)
            max_loc_ts = ts.get_max_value_for_field("background/filtered_location", "data.ts")
            if max_loc_ts == -1:
                logging.warning("No entries for user %s, early return " % self.user_id)
                return
            if max_loc_ts > start_ts:
                # We have locations, but no trips from them. That seems wrong.
                # But we should get there eventually and then we will have trips.
                logging.warning("No analysis has been done on recent points! max_loc_ts %s > start_ts %s, early return" %
                                (max_loc_ts, start_ts))
                return
            trip_gj_list = self.get_trip_list_for_seven_days(max_loc_ts)
        day_list_bins = self.bin_into_days_by_local_time(trip_gj_list)
        uc = enua.UserCache.getUserCache(self.user_id)

        for day, day_gj_list in day_list_bins.items():
            logging.debug("Adding %s trips for day %s" % (len(day_gj_list), day))
            uc.putDocument("diary/trips-%s"%day, day_gj_list)

        valid_key_list = ["diary/trips-%s"%day for day in day_list_bins.keys()]
        self.delete_obsolete_entries(uc, valid_key_list)

    def storeCommonTripsToCache(self, time_query):
        """
        Determine which set of common trips to send to the usercache.
        As of now we will run the pipeline on the full set of data and send that up
        """
        tour_model = esdtmpq.get_tour_model(self.user_id)
        uc = enua.UserCache.getUserCache(self.user_id)
        logging.debug("Adding common trips for day %s" % str(pydt.date.today()))
        # We don't really support day-specific common trips in any other format
        # So it doesn't make sense to support it only for the cache, where it will
        # accumulate on the phone uselessly
        # IF we are going to support versioned common trips objects, we should
        # do even outside the cache code
        # uc.putDocument("common_trips-%s" % str(pydt.date.today()),  tour_model)
        # valid_key_list = ["common_trips-%s" % str(pydt.date.today())]
        # self.delete_obsolete_entries(uc, valid_key_list)
        logging.debug("About to save model with len(places) = %d and len(trips) = %d" %
            (len(tour_model["common_places"]), len(tour_model["common_trips"])))
        uc.putDocument("common-trips", tour_model)

    def storeConfigsToCache(self, time_query):
        """
        Iterate through all configs, figure out the correct version to push to
        the phone, and do so.
        """
        uc = enua.UserCache.getUserCache(self.user_id)
        return eacc.save_all_configs(self.user_id, time_query)


    def get_oldest_valid_ts(self, start_ts):
        """
        Get the oldest valid timestamp that we want to include.
        Currently, this is 7 days ago, but we can change it by modifying this
            one location.
        """
        return start_ts - 7 * 24 * 60 * 60 # 7 days in seconds

    def delete_obsolete_entries(self, uc, valid_key_list):
        for key in self.get_obsolete_entries(uc, valid_key_list):
            uc.clearObsoleteDocument(key)

    def get_obsolete_entries(self, uc, valid_key_list):
        """
        Delete the obsolete entries from the usercache
        """
        # Get all the current entries from the usercache
        curr_key_list = uc.getDocumentKeyList()
        # Technically, we could look to find all keys that are before the
        # current one, but that would imply that we need to set an ordering on
        # the keys. The current key generation should work fine with
        # lexicographic ordering, but at the same time, this seems much easier
        # and safer to deal with.
        valid_key_list.append('config/sensor_config')
        valid_key_list.append('config/sync_config')
        valid_key_list.append('config/consent')
        logging.debug("curr_key_list = %s, valid_key_list = %s" %
           (curr_key_list, valid_key_list))
        to_del_keys = set(curr_key_list) - set(valid_key_list)
        logging.debug("obsolete keys are: %s" % to_del_keys)
        return to_del_keys

    def get_trip_list_for_seven_days(self, start_ts):
        seventy_two_hours_ago_ts = self.get_oldest_valid_ts(start_ts)
        # TODO: This is not strictly accurate, since it will skip trips that were in a later timezone but within the
        # same requested date range.
        trip_gj_list = gfc.get_geojson_for_ts(self.user_id, seventy_two_hours_ago_ts, start_ts)
        logging.debug("Found %s trips in seven days starting from %s (%s)" % (len(trip_gj_list), start_ts, pydt.datetime.utcfromtimestamp(start_ts).isoformat()))
        return trip_gj_list

    @staticmethod
    def get_local_day_from_fmt_time(trip):
        """
        Returns the day, formatted as a human readable string.
        i.e. "2016-01-01"
        """
        return trip.start_fmt_time.split("T")[0]

    @staticmethod
    def get_local_day_from_local_dt(trip):
        """
        Returns the day, formatted as a human readable string.
        i.e. "2016-01-01"
        """
        ld = trip.start_local_dt
        return ("%04d" % ld.year) + "-" + ("%02d" % ld.month) + "-" + ("%02d" % ld.day)

    def bin_into_days_by_local_time(self, trip_gj_list):
        """
        While binning the trips into days, we really want to use local time. This is because that is what the user
        experienced. For example, consider a hypothetical user who travels from the West Coast to the East Coast of the
        US, leaving at 11pm local time, which would be 2am Eastern time. When the user looks at their timeline the next
        day, when they are on East Coast local time, they still want to see the trip to the airport as occurring on
        the previous day, not the same day, because that is when they experienced it. So, as corny as it seems, we are
        going to use the start and end formatted times, split them and extract the date part.
        :param trip_gj_list: List of trips
        :return: Map of date string -> trips that start or end on date
        """
        ret_val = {}

        for trip_gj in trip_gj_list:
            trip = ecwt.Trip(trip_gj.properties)
            # TODO: Consider extending for both start and end
            day_string = BuiltinUserCacheHandler.get_local_day_from_fmt_time(trip)
            if day_string not in ret_val:
                ret_val[day_string] = [trip_gj]
            else:
                list_for_curr_day = ret_val[day_string]
                list_for_curr_day.append(trip_gj)

        logging.debug("After binning, we have %s bins, of which %s are empty" %
                      (len(ret_val), len([ds for ds,dl in ret_val.items() if len(dl) == 0])))
        return ret_val
