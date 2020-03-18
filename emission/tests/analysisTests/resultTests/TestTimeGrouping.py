from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import logging
import pandas as pd
import uuid
import arrow
import dateutil.tz as tz

# Our imports
import emission.core.get_database as edb
import emission.tests.common as etc

import emission.analysis.result.metrics.time_grouping as earmt
import emission.analysis.result.metrics.simple_metrics as earmts
import emission.analysis.config as eac

import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.section as ecws
import emission.core.wrapper.modestattimesummary as ecwms
import emission.core.wrapper.modeprediction as ecwm
import emission.core.wrapper.localdate as ecwl

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.decorations.local_date_queries as esdl

import emission.tests.common as etc

PST = "America/Los_Angeles"
EST = "America/New_York"
IST = "Asia/Calcutta"
BST = "Europe/London"

class TestTimeGrouping(unittest.TestCase):
    def setUp(self):
        self.testUUID = uuid.uuid4()
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)

    def tearDown(self):
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.testUUID})

    def testLocalGroupBy(self):
        self.assertEqual(earmt._get_local_group_by(earmt.LocalFreq.DAILY),
                         ['start_local_dt_year', 'start_local_dt_month',
                          'start_local_dt_day'])
        self.assertEqual(earmt._get_local_group_by(earmt.LocalFreq.YEARLY),
                         ['start_local_dt_year'])
        with self.assertRaises(AssertionError):
            earmt._get_local_group_by("W")

    def testLocalKeyToFillFunction(self):
        self.assertEqual(earmt._get_local_key_to_fill_fn(earmt.LocalFreq.DAILY),
                         earmt.local_dt_fill_times_daily)
        self.assertEqual(earmt._get_local_key_to_fill_fn(earmt.LocalFreq.YEARLY),
                         earmt.local_dt_fill_times_yearly)
        with self.assertRaises(AssertionError):
            earmt._get_local_key_to_fill_fn("W")

    def _createTestSection(self, start_ardt, start_timezone):
        section = ecws.Section()
        self._fillDates(section, "start_", start_ardt, start_timezone)
        # Hackily fill in the end with the same values as the start
        # so that the field exists
        # in cases where the end is important (mainly for range timezone
        # calculation with local times), it can be overridden using _fillDates
        # from the test case
        self._fillDates(section, "end_", start_ardt, start_timezone)
        logging.debug("created section %s" % (section.start_fmt_time))

        entry = ecwe.Entry.create_entry(self.testUUID,
                                        eac.get_section_key_for_analysis_results(),
                                        section, create_id=True)
        self.ts.insert(entry)
        return entry

    def _fillDates(self, object, prefix, ardt, timezone):
        object["%sts" % prefix] = ardt.timestamp
        object["%slocal_dt" % prefix] = esdl.get_local_date(ardt.timestamp,
                                                     timezone)
        object["%sfmt_time" % prefix] = ardt.to(timezone).isoformat()
        logging.debug("After filling entries, keys are %s" % list(object.keys()))
        return object

    def testLocalDtFillTimesDailyOneTz(self):
        key = (2016, 5, 3)
        test_section_list = []
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,6, tzinfo=tz.gettz(PST)),
                                    PST))
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,10, tzinfo=tz.gettz(PST)),
                                    PST))
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,23, tzinfo=tz.gettz(PST)),
                                    PST))

        section_group_df = self.ts.to_data_df(eac.get_section_key_for_analysis_results(),
                                              test_section_list)
        logging.debug("First row of section_group_df = %s" % section_group_df.iloc[0])
        self.assertEqual(earmt._get_tz(section_group_df), PST)

        ms = ecwms.ModeStatTimeSummary()
        earmt.local_dt_fill_times_daily(key, section_group_df, ms)
        logging.debug("before starting checks, ms = %s" % ms)
        self.assertEqual(ms.ts, 1462258800)
        self.assertEqual(ms.local_dt.day, 3)
        self.assertEqual(ms.local_dt.timezone, PST)

    def testLocalDtFillTimesDailyMultiTzGoingWest(self):
        key = (2016, 5, 3)
        test_section_list = []
        # This is perhaps an extreme use case, but it is actually a fairly
        # common one with air travel

        # Step 1: user leaves Delhi at 1am on the 3rd for JFK on the non-stop
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,1, tzinfo=tz.gettz(IST)),
                                    IST))
        # non-stop takes 15 hours, so she arrives in New York at 16:00 IST = 6:30am EDT
        # (taking into account the time difference)

        # Step 2: user leaves JFK for SFO at 7am EST on a non-stop
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,7, tzinfo=tz.gettz(EST)),
                                    EST))

        # cross-country flight takes 8 hours, so she arrives in SFO at 15:00 EDT
        # = 12:00 PDT
        test_section_list[1]['data'] = self._fillDates(test_section_list[1].data, "end_",
                        arrow.Arrow(2016,5,3,15,tzinfo=tz.gettz(EST)),
                        PST)

        # Step 2: user starts a trip out of SFO a midnight of the 4th PST
        # (earliest possible trip)
        # for our timestamp algo to be correct, this has to be after the
        # timestamp for the range
        next_day_first_trip = self._createTestSection(
            arrow.Arrow(2016,5,4,0, tzinfo=tz.gettz(PST)),
                                    PST)

        section_group_df = self.ts.to_data_df(eac.get_section_key_for_analysis_results(),
                                              test_section_list)

        # Timestamps are monotonically increasing
        self.assertEqual(section_group_df.start_ts.tolist(),
                         [1462217400, 1462273200])
        self.assertEqual(next_day_first_trip.data.start_ts, 1462345200)

        # The timezone for the end time is IST since that's where we started
        # the first trip
        self.assertEqual(earmt._get_tz(section_group_df), IST)

        ms = ecwms.ModeStatTimeSummary()
        earmt.local_dt_fill_times_daily(key, section_group_df, ms)
        logging.debug("before starting checks, ms = %s" % ms)

        # The end of the period is the end of the day in PST. So that we can
        # capture trip home from the airport, etc.
        # The next trip must start from the same timezone
        # if a trip straddles two timezones, we need to decide how the metrics
        # are split. A similar issue occurs when the trip straddles two days.
        # We have arbitrarily decided to bucket by start_time, so we follow the
        # same logic and bucket by the timezone of the start time.
        #
        # So the bucket for this day ends at the end of the day in EDT.
        # If we included any trips after noon in SF, e.g. going home from the
        # aiport, then it would extend to midnight PDT.
        #
        # The main argument that I'm trying to articulate is that we need to
        # come up with a notion of when the bucket ended. To some extent, we can
        # set this arbitrarily between the end of the last trip on the 3rd and the
        # and the start of the first trip on the 4th.
        #
        # Picking midnight on the timezone of the last trip on the 3rd is
        # reasonable since we know that no trips have started since the last
        # trip on the 3rd to the midnight of the 3rd EST.

        # So the worry here is that the first trip on the next day may be on
        # next day in the end timezone of the trip but on the same day in the
        # start timezone of the trip
        # e.g. reverse trip
        # maybe using the end of the section is best after all

        self.assertEqual(ms.ts, 1462213800)
        self.assertEqual(ms.local_dt.day, 3)
        self.assertEqual(ms.local_dt.timezone, IST)
        self.assertGreater(next_day_first_trip.data.start_ts, ms.ts)

    def testLocalDtFillTimesDailyMultiTzGoingEast(self):
        key = (2016, 5, 3)
        test_section_list = []
        # This is perhaps an extreme use case, but it is actually a fairly
        # common one with air travel

        # Step 1: user leaves SFO at 1am on the 3rd for JFK on a cross-country flight
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,1, tzinfo=tz.gettz(PST)),
                                    PST))
        # cross-country takes 8 hours, so she arrives in New York at 9:00 IST = 12:00am EDT
        # (taking into account the time difference)
        test_section_list[0]['data'] = self._fillDates(test_section_list[0].data, "end_",
                        arrow.Arrow(2016,5,3,9,tzinfo=tz.gettz(PST)),
                        EST)

        # Step 2: user leaves JFK for LHR at 1pm EST.
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,13, tzinfo=tz.gettz(EST)),
                                    EST))

        # cross-atlantic flight takes 7 hours, so she arrives at LHR at 8:00pm EDT
        # = 2am on the 4th local time
        test_section_list[1]['data'] = self._fillDates(test_section_list[1].data, "end_",
                        arrow.Arrow(2016,5,3,21,tzinfo=tz.gettz(EST)),
                        BST)

        # Then, she catches the train from the airport to her hotel in London
        # at 3am local time = 9:00pm EST
        # So as per local time, this is a new trip
        #
        # This clearly indicates why we need to use the timezone of the end of
        # last section to generate the timestamp for the range. If we use the
        # timezone of the beginning of the trip, we will say that the range ends
        # at midnight EST. But then it should include the next_day_first_trip,
        # which starts at 9pm EST, but it does not.
        # So we should use midnight BST instead. Note that midnight BST was
        # actually during the trip, but then it is no different from a regular
        # trip (in one timezone) where the trip spans the date change
        next_day_first_trip = self._createTestSection(
            arrow.Arrow(2016,5,4,3, tzinfo=tz.gettz(BST)),
            BST)

        section_group_df = self.ts.to_data_df(eac.get_section_key_for_analysis_results(),
            test_section_list)
        logging.debug("first row is %s" % section_group_df.loc[0])

        # Timestamps are monotonically increasing
        self.assertEqual(section_group_df.start_ts.tolist(),
                         [1462262400, 1462294800])

        # The timezone for the end time is PST since that's where we started
        # the first trip from
        self.assertEqual(earmt._get_tz(section_group_df), PST)

        ms = ecwms.ModeStatTimeSummary()
        earmt.local_dt_fill_times_daily(key, section_group_df, ms)
        logging.debug("before starting checks, ms = %s" % ms)

        self.assertEqual(ms.ts, 1462258800)
        self.assertEqual(ms.local_dt.day, 3)
        self.assertEqual(ms.local_dt.timezone, PST)

        # This test fails if it is not BST
        self.assertGreater(next_day_first_trip.data.start_ts, ms.ts)

    # I am not testing testGroupedToSummaryTime because in order to generate the
    # time grouped df, I need to group the sections anyway, and then I might as we

    def _fillModeDistanceDuration(self, section_list):
        for i, s in enumerate(section_list):
            dw = s.data
            dw.sensed_mode = ecwm.PredictedModeTypes.BICYCLING
            dw.duration = (i + 1) * 100
            dw.distance = (i + 1) * 1000
            s['data'] = dw
            self.ts.update(s)


    def testGroupedByOneLocalDayOneUTCDay(self):
        key = (2016, 5, 3)
        test_section_list = []
        #
        # Since PST is UTC-7, all of these will be in the same UTC day
        # 13:00, 17:00, 21:00
        # so we expect the local date and UTC bins to be the same
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,6, tzinfo=tz.gettz(PST)),
                                    PST))
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,10, tzinfo=tz.gettz(PST)),
                                    PST))
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,14, tzinfo=tz.gettz(PST)),
                                    PST))

        self._fillModeDistanceDuration(test_section_list)
        logging.debug("durations = %s" %
                      [s.data.duration for s in test_section_list])

        summary_ts_dict = earmt.group_by_timestamp(self.testUUID,
                                           arrow.Arrow(2016,5,1).timestamp,
                                           arrow.Arrow(2016,6,1).timestamp,
                                           'd', [earmts.get_count])
        summary_ld_dict = earmt.group_by_local_date(self.testUUID,
                                               ecwl.LocalDate({'year': 2016, 'month': 5}),
                                               ecwl.LocalDate({'year': 2016, 'month': 6}),
                                               earmt.LocalFreq.DAILY, [earmts.get_count])

        summary_ts_last = summary_ts_dict["last_ts_processed"]
        summary_ld_last = summary_ld_dict["last_ts_processed"]

        summary_ts = summary_ts_dict["result"][0]
        summary_ld = summary_ld_dict["result"][0]

        self.assertEqual(summary_ts_last, arrow.Arrow(2016,5,3,14, tzinfo=tz.gettz(PST)).timestamp)
        self.assertEqual(summary_ld_last, arrow.Arrow(2016,5,3,14, tzinfo=tz.gettz(PST)).timestamp)

        self.assertEqual(len(summary_ts), len(summary_ld)) # local date and UTC results are the same
        self.assertEqual(len(summary_ts), 1) # spans one day
        self.assertEqual(summary_ts[0].BICYCLING, summary_ld[0].BICYCLING)
        self.assertEqual(summary_ts[0].BICYCLING, 3)
        # Note that the timestamps are not guaranteed to be equal since
        # the UTC range starts at midnight UTC while the local time range
        # starts at midnight PDT
        # self.assertEqual(summary_ts[0].ts, summary_ld[0].ts)
        self.assertEqual(summary_ts[0].ts, 1462233600)
        self.assertEqual(summary_ld[0].ts, 1462258800)
        self.assertEqual(summary_ts[0].local_dt.day, 3)
        self.assertEqual(summary_ts[0].local_dt.day, summary_ld[0].local_dt.day)


    def testGroupedByOneLocalDayMultiUTCDay(self):
        key = (2016, 5, 3)
        test_section_list = []

        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,6, tzinfo=tz.gettz(PST)),
                                    PST))
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,10, tzinfo=tz.gettz(PST)),
                                    PST))
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,23, tzinfo=tz.gettz(PST)),
                                    PST))

        self._fillModeDistanceDuration(test_section_list)
        logging.debug("durations = %s" %
                      [s.data.duration for s in test_section_list])

        # There's only one local date, so it will be consistent with
        # results in testGroupedByOneLocalDayOneUTCDay
        summary_ld_dict = earmt.group_by_local_date(self.testUUID,
                                               ecwl.LocalDate({'year': 2016, 'month': 5}),
                                               ecwl.LocalDate({'year': 2016, 'month': 6}),
                                               earmt.LocalFreq.DAILY, [earmts.get_count])

        summary_ld = summary_ld_dict["result"][0]
        summary_ld_last = summary_ld_dict["last_ts_processed"]
        self.assertEqual(summary_ld_last,
                         arrow.Arrow(2016,5,3,23, tzinfo=tz.gettz(PST)).timestamp)
        self.assertEqual(len(summary_ld), 1) # spans one day
        self.assertEqual(summary_ld[0].BICYCLING, 3)
        self.assertEqual(summary_ld[0].ts, 1462258800)
        self.assertEqual(summary_ld[0].local_dt.day, 3)

        summary_ts_dict = earmt.group_by_timestamp(self.testUUID,
                                           arrow.Arrow(2016,5,1).timestamp,
                                           arrow.Arrow(2016,6,1).timestamp,
                                           'd', [earmts.get_count])
        summary_ts = summary_ts_dict["result"][0]
        summary_ts_last = summary_ts_dict["last_ts_processed"]

        # But 23:00 PDT is 6am on the 4th in UTC,
        # so the results are different for this
        self.assertEqual(summary_ts_last,
                         arrow.Arrow(2016,5,3,23, tzinfo=tz.gettz(PST)).timestamp)
        self.assertEqual(len(summary_ts), 2) # spans two days in UTC
        self.assertEqual(summary_ts[0].BICYCLING, 2) # 2 trips on the first day
        self.assertEqual(summary_ts[1].BICYCLING, 1) # 1 trips on the second day
        self.assertEqual(summary_ts[0].local_dt.day, 3) # because it is the second in UTC
        self.assertEqual(summary_ts[1].local_dt.day, 4) # because it is the second in UTC
        self.assertEqual(summary_ts[0].ts, 1462233600) # timestamp for midnight 3nd May
        self.assertEqual(summary_ts[1].ts, 1462320000) # timestamp for midnight 4rd May

    def testGroupedByOneLocalDayMultiTzGoingWest(self):
        key = (2016, 5, 3)
        test_section_list = []
        # This is perhaps an extreme use case, but it is actually a fairly
        # common one with air travel

        # Step 1: user leaves Delhi at 1am on the 3rd for JFK on the non-stop
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,1, tzinfo=tz.gettz(IST)),
                                    IST))
        # non-stop takes 15 hours, so she arrives in New York at 16:00 IST = 6:30am EDT
        # (taking into account the time difference)

        # Step 2: user leaves JFK for SFO at 7am EST on a non-stop
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,7, tzinfo=tz.gettz(EST)),
                                    EST))

        # Step 3: user starts a trip out of SFO a midnight of the 4th PST
        # (earliest possible trip)
        # for our timestamp algo to be correct, this has to be after the
        # timestamp for the range
        next_day_first_trip = self._createTestSection(
            arrow.Arrow(2016,5,4,0, tzinfo=tz.gettz(PST)),
        PST)

        self._fillModeDistanceDuration(test_section_list)
        self._fillModeDistanceDuration([next_day_first_trip])
        logging.debug("durations = %s" %
                      [s.data.duration for s in test_section_list])

        summary_ts_dict = earmt.group_by_timestamp(self.testUUID,
                                           arrow.Arrow(2016,5,1).timestamp,
                                           arrow.Arrow(2016,6,1).timestamp,
                                           'd', [earmts.get_count])

        summary_ts_last = summary_ts_dict["last_ts_processed"]
        summary_ts = summary_ts_dict["result"][0]

        logging.debug(summary_ts)

        self.assertEqual(summary_ts_last,
                         arrow.Arrow(2016,5,4,0, tzinfo=tz.gettz(PST)).timestamp) # spans two days in UTC
        self.assertEqual(len(summary_ts), 3) # spans two days in UTC
        self.assertEqual(summary_ts[0].BICYCLING, 1) # trip leaving India
        self.assertEqual(summary_ts[1].BICYCLING, 1) # trip from New York
        self.assertEqual(summary_ts[2].BICYCLING, 1) # trip in SF
        self.assertEqual(summary_ts[0].local_dt.day, 2) # because it is the second in UTC
        self.assertEqual(summary_ts[1].local_dt.day, 3)
        self.assertEqual(summary_ts[2].local_dt.day, 4)
        self.assertEqual(summary_ts[0].local_dt.hour, 0) # timestamp fills out all vals
        self.assertEqual(summary_ts[0].ts, 1462147200) # timestamp for start of 2nd May in UTC
        self.assertEqual(summary_ts[1].ts, 1462233600) # timestamp for start of 2nd May in UTC

        # There's only one local date, but it starts in IST this time
        summary_ld_dict = earmt.group_by_local_date(self.testUUID,
                                               ecwl.LocalDate({'year': 2016, 'month': 5}),
                                               ecwl.LocalDate({'year': 2016, 'month': 6}),
                                               earmt.LocalFreq.DAILY, [earmts.get_count])

        summary_ld = summary_ld_dict["result"][0]
        summary_ld_last = summary_ld_dict["last_ts_processed"]

        self.assertEqual(summary_ld_last,
                         arrow.Arrow(2016,5,4,0, tzinfo=tz.gettz(PST)).timestamp) # spans two days in UTC
        self.assertEqual(len(summary_ld), 2) # spans one day + 1 trip at midnight
        self.assertEqual(summary_ld[0].BICYCLING, 2) # two plane trips
        self.assertEqual(summary_ld[1].BICYCLING, 1) # trip SFO
        self.assertEqual(summary_ld[0].ts, 1462213800) # start of the 3rd in IST
        self.assertEqual(summary_ld[0].local_dt.day, 3)
        self.assertEqual(summary_ld[1].ts, 1462345200) # start of the 4th in IST
        self.assertEqual(summary_ld[1].local_dt.day, 4)

    def testGroupedByOneLocalDayMultiTzGoingEast(self):
        key = (2016, 5, 3)
        test_section_list = []
        # This is perhaps an extreme use case, but it is actually a fairly
        # common one with air travel

        # Step 1: user leaves SFO at 1am on the 3rd for JFK on a cross-country flight
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,1, tzinfo=tz.gettz(PST)),
                                    PST))
        # cross-country takes 8 hours, so she arrives in New York at 9:00 IST = 12:00am EDT
        # (taking into account the time difference)
        test_section_list[0]['data'] = self._fillDates(test_section_list[0].data, "end_",
                                                       arrow.Arrow(2016,5,3,9,tzinfo=tz.gettz(PST)),
                                                       EST)

        # Step 2: user leaves JFK for LHR at 1pm EST.
        test_section_list.append(
            self._createTestSection(arrow.Arrow(2016,5,3,13, tzinfo=tz.gettz(EST)),
                                    EST))

        # cross-atlantic flight takes 7 hours, so she arrives at LHR at 8:00pm EDT
        # = 2am on the 4th local time
        test_section_list[1]['data'] = self._fillDates(test_section_list[1].data, "end_",
                                                       arrow.Arrow(2016,5,3,21,tzinfo=tz.gettz(EST)),
                                                       BST)

        # Then, she catches the train from the airport to her hotel in London
        # at 3am local time = 9:00pm EST
        # So as per local time, this is a new trip
        #
        # This clearly indicates why we need to use the timezone of the end of
        # last section to generate the timestamp for the range. If we use the
        # timezone of the beginning of the trip, we will say that the range ends
        # at midnight EST. But then it should include the next_day_first_trip,
        # which starts at 9pm EST, but it does not.
        # So we should use midnight BST instead. Note that midnight BST was
        # actually during the trip, but then it is no different from a regular
        # trip (in one timezone) where the trip spans the date change
        next_day_first_trip = self._createTestSection(
            arrow.Arrow(2016,5,4,3, tzinfo=tz.gettz(BST)),
            BST)

        self._fillModeDistanceDuration(test_section_list)
        self._fillModeDistanceDuration([next_day_first_trip])

        logging.debug("durations = %s" %
                      [s.data.duration for s in test_section_list])

        summary_dict = earmt.group_by_timestamp(self.testUUID,
                                           arrow.Arrow(2016,5,1).timestamp,
                                           arrow.Arrow(2016,6,1).timestamp,
                                           'd', [earmts.get_count])
        summary_last = summary_dict["last_ts_processed"]
        summary = summary_dict["result"][0]

        logging.debug(summary)

        self.assertEqual(summary_last,
                         arrow.Arrow(2016,5,4,3, tzinfo=tz.gettz(BST)).timestamp)

        self.assertEqual(len(summary), 2) # spans two days in UTC
        self.assertEqual(summary[0].BICYCLING, 2) # trip leaving SFO and JFK
        self.assertEqual(summary[1].BICYCLING, 1) # trip in GMT
        self.assertEqual(summary[0].local_dt.day, 3) # because it is the second in UTC
        self.assertEqual(summary[1].local_dt.day, 4)
        self.assertEqual(summary[0].local_dt.hour, 0) # timestamp fills out all vals
        self.assertEqual(summary[0].ts, 1462233600) # timestamp for start of 2nd May in UTC
        self.assertEqual(summary[1].ts, 1462320000) # timestamp for start of 2nd May in UTC

        # There's only one local date, but it starts in IST this time
        summary_ld_dict = earmt.group_by_local_date(self.testUUID,
                                               ecwl.LocalDate({'year': 2016, 'month': 5}),
                                               ecwl.LocalDate({'year': 2016, 'month': 6}),
                                               earmt.LocalFreq.DAILY, [earmts.get_count])

        summary_ld_last = summary_ld_dict["last_ts_processed"]
        summary_ld = summary_ld_dict["result"][0]

        self.assertEqual(summary_ld_last,
                         arrow.Arrow(2016,5,4,3, tzinfo=tz.gettz(BST)).timestamp)
        self.assertEqual(len(summary_ld), 2) # spans one day + 1 trip on the next day
        self.assertEqual(summary_ld[0].BICYCLING, 2) # two plane trips
        self.assertEqual(summary_ld[1].BICYCLING, 1) # trip SFO
        self.assertEqual(summary_ld[0].ts, 1462258800) # start of the 3rd in IST
        self.assertEqual(summary_ld[0].local_dt.day, 3)
        self.assertEqual(summary_ld[1].ts, 1462316400) # start of the 4th in BST
        self.assertEqual(summary_ld[1].local_dt.day, 4)


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
