from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import range
from builtins import *
import logging
import random
import unittest
import uuid

import arrow
import attrdict as ad
import dateutil.tz as tz

import emission.analysis.result.metrics.simple_metrics as earmts
import emission.analysis.result.metrics.time_grouping as earmt
import emission.core.get_database as edb
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.modeprediction as ecwm
import emission.core.wrapper.section as ecws
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.decorations.local_date_queries as esdl
import emission.storage.timeseries.abstract_timeseries as esta

import emission.net.ext_service.habitica.proxy as proxy
import emission.net.ext_service.habitica.executor as autocheck
import emission.net.ext_service.habitica.auto_tasks.task as enehat

PST = "America/Los_Angeles"


class TestHabiticaAutocheck(unittest.TestCase):
  def setUp(self):
    #load test user
    self.testUUID = uuid.uuid4()
    autogen_string = randomGen()
    autogen_email = autogen_string + '@test.com'
    self.sampleAuthMessage1 = {'username': autogen_string, 'email': autogen_email, 
      'password': "test", 'our_uuid': self.testUUID}
    sampleAuthMessage1Ad = ad.AttrDict(self.sampleAuthMessage1)
    proxy.habiticaRegister(sampleAuthMessage1Ad.username, sampleAuthMessage1Ad.email,
                           sampleAuthMessage1Ad.password, sampleAuthMessage1Ad.our_uuid)

    self.ts = esta.TimeSeries.get_time_series(self.testUUID)
    bike_habit = {'type': "habit", 'text': "Bike", 'up': True, 'down': False, 'priority': 2}
    bike_habit_id = proxy.create_habit(self.testUUID, bike_habit)
    walk_habit = {'type': "habit", 'text': "Walk", 'up': True, 'down': False, 'priority': 2}
    walk_habit_id = proxy.create_habit(self.testUUID, walk_habit)
    logging.debug("in setUp, result = %s" % self.ts)


  def tearDown(self):
    edb.get_analysis_timeseries_db().remove({'user_id': self.testUUID})
    del_result = proxy.habiticaProxy(self.testUUID, "DELETE",
                                     "/api/v3/user",
                                     {'password': "test"})
    edb.get_habitica_db().remove({'user_id': self.testUUID})
    logging.debug("in tearDown, result = %s" % del_result)


  def testCreateNewHabit(self):
    new_habit = {'type': "habit", 'text': randomGen(),
                 'notes': 'AUTOCHECK: {"mapper": "active_distance",'
                          '"args": {"walk_scale": 1000, "bike_scale": 3000}}'}
    habit_id = proxy.create_habit(self.testUUID, new_habit)
    logging.debug("in testCreateNewHabit, the new habit id is = %s" % habit_id)
    #Get user's list of habits and check that new habit is there
    response = proxy.habiticaProxy(self.testUUID, 'GET', "/api/v3/tasks/user?type=habits", None)
    logging.debug("in testCreateNewHabit, GET habits response = %s" % response)
    habits = response.json()
    logging.debug("in testCreateNewHabit, this user's list of habits = %s" % habits)
    self.assertTrue(habit['_id'] == habit_id for habit in habits['data'])
    self.assertTrue(habit['text'] == new_habit['text'] for habit in habits['data'])
    

  def _createTestSection(self, start_ardt, start_timezone):
    section = ecws.Section()
    self._fillDates(section, "start_", start_ardt, start_timezone)
    # Hackily fill in the end with the same values as the start
    # so that the field exists
    # in cases where the end is important (mainly for range timezone
    # calculation with local times), it can be overridden using _fillDates
    # from the test case
    self._fillDates(section, "end_", start_ardt, start_timezone)
    #logging.debug("created section %s" % (section.start_fmt_time))
    entry = ecwe.Entry.create_entry(self.testUUID, esda.INFERRED_SECTION_KEY,
                                    section, create_id=True)
    self.ts.insert(entry)
    return entry

  def _fillDates(self, object, prefix, ardt, timezone):
    object["%sts" % prefix] = ardt.timestamp
    object["%slocal_dt" % prefix] = esdl.get_local_date(ardt.timestamp, timezone)
    object["%sfmt_time" % prefix] = ardt.to(timezone).isoformat()
    #logging.debug("After filling entries, keys are %s" % object.keys())
    return object

  def _fillModeDistanceDuration(self, section_list):
    for i, s in enumerate(section_list):
      dw = s.data
      dw.sensed_mode = ecwm.PredictedModeTypes.BICYCLING
      dw.duration = (i + 1) * 100
      dw.distance = (i + 1.5) * 1000
      s['data'] = dw
      self.ts.update(s)


  def testAutomaticRewardActiveTransportation(self):
    # Create a task that we can retrieve later

    self.new_task_text = randomGen()
    new_habit = {'type': "habit", 'text': self.new_task_text,
                 'notes': 'AUTOCHECK: {"mapper": "active_distance",'
                          '"args": {"walk_scale": 1000, "bike_scale": 3000}}'}
    habit_id = proxy.create_habit(self.testUUID, new_habit)

    self.dummy_task = enehat.Task()
    self.dummy_task.task_id = habit_id
    logging.debug("in testAutomaticRewardActiveTransportation,"
        "the new habit id is = %s and task is %s" % (habit_id, self.dummy_task))

    #Create test data -- code copied from TestTimeGrouping
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
    #logging.debug("durations = %s" % [s.data.duration for s in test_section_list])

    summary_ts = earmt.group_by_timestamp(self.testUUID,
                                       arrow.Arrow(2016,5,1).timestamp,
                                       arrow.Arrow(2016,6,1).timestamp,
                                       None, [earmts.get_distance])
    logging.debug("in testAutomaticRewardActiveTransportation, result = %s" % summary_ts)
    
    #Get user data before scoring
    user_before = autocheck.get_task_state(self.testUUID, self.dummy_task)
    self.assertIsNone(user_before)

    # Needed to work, otherwise sections from may won't show up in the query!
    modification = {"last_timestamp": arrow.Arrow(2016,5,1).timestamp, "bike_count": 0, "walk_count":0}
    autocheck.save_task_state(self.testUUID, self.dummy_task, modification)

    user_before = autocheck.get_task_state(self.testUUID, self.dummy_task)
    self.assertEqual(int(user_before['bike_count']), 0)

    habits_before = proxy.habiticaProxy(self.testUUID, 'GET', "/api/v3/tasks/user?type=habits", None).json()
    bike_pts_before = [habit['history'] for habit in habits_before['data'] if habit['text'] == self.new_task_text]
    #Score points
    autocheck.give_points_for_all_tasks(self.testUUID)
    #Get user data after scoring and check results
    user_after = autocheck.get_task_state(self.testUUID, self.dummy_task)
    self.assertEqual(int(user_after['bike_count']),1500)
    habits_after = proxy.habiticaProxy(self.testUUID, 'GET', "/api/v3/tasks/user?type=habits", None).json()
    bike_pts_after = [habit['history'] for habit in habits_after['data'] if habit['text'] == self.new_task_text]
    self.assertTrue(len(bike_pts_after[0]) - len(bike_pts_before[0]) == 2)

  def testResetActiveTransportation(self):
    self.testAutomaticRewardActiveTransportation()

    #Get user data before resetting
    user_before = autocheck.get_task_state(self.testUUID, self.dummy_task)
    self.assertEqual(int(user_before['bike_count']), 1500)

    habits_before = proxy.habiticaProxy(self.testUUID, 'GET', "/api/v3/tasks/user?type=habits", None).json()
    bike_pts_before = [habit['history'] for habit in habits_before['data'] if habit['text'] == self.new_task_text]

    #Reset
    reset_ts = arrow.Arrow(2016,5,3,9).timestamp
    autocheck.reset_all_tasks_to_ts(self.testUUID, reset_ts, is_dry_run=False)

    # Check timestamp 
    user_after = autocheck.get_task_state(self.testUUID, self.dummy_task)
    self.assertEqual(int(user_after['last_timestamp']), reset_ts)

    # Re-score points
    # This should give points for the second and third sections
    # So I expect to see an additional distance of 2.5 + 3.5 km = 6km
    autocheck.give_points_for_all_tasks(self.testUUID)

    #Get user data after scoring and check results
    # We already had bike_count = 1500, and this is a round number, so it
    # should continue to be 1500
    user_after = autocheck.get_task_state(self.testUUID, self.dummy_task)
    self.assertEqual(int(user_after['bike_count']), 0)

    # and we should have 6 points more?
    habits_after = proxy.habiticaProxy(self.testUUID, 'GET', "/api/v3/tasks/user?type=habits", None).json()
    bike_pts_after = [habit['history'] for habit in habits_after['data'] if habit['text'] == self.new_task_text]
    logging.debug("bike_pts_after = %s" % (len(bike_pts_after[0]) - len(bike_pts_before[0])))
    self.assertTrue(len(bike_pts_after[0]) - len(bike_pts_before[0]) == 3)

def randomGen():
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    length = 5
    string = ""
    for i in range(length):
      next_index = random.randrange(len(alphabet))
      string = string + alphabet[next_index]
    return string


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
