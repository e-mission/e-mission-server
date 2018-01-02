import emission.core.get_database as edb
import pandas as pd
from uuid import UUID
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt
import arrow

class TierSys:
  def __init__(self, tsid):
    self.tsid = tsid
    self.tiers = {}
    for i in range(1, 6):
      self.addTier(i)
    
  def addTier(self, rank):
      self.tiers[rank] = Tier(rank)

  def computeRanks(self, last_ts, n):
    #TODO: FINISH
      """
      Get all users from DB
      sort them by carbon metric
      compute percentiles
      set boundaries
      return list of n lists for n ranks, 
      with the tiers sorted in descending order."""
      user_carbon_map = {} # Map from user_id to carbon val.
      num_users = len(all_users.iloc)
      all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"user_email":1, "uuid": 1, "_id": 0})))
      for i in range(1, num_users+1):
          user_id = all_users.iloc[i].UUID
          user_carbon_map[user_id] = self.computeCarbon(user_id, last_ts)

      # Sort and partition users by carbon metric.    
      user_carbon_tuples_sorted = sorted(user_carbon_map.iteritems(), key=lambda (k,v): (v,k)) # Sorted list by value of dict tuples.
      user_carbon_sorted = [i[0] for i in user_carbon_tuples_sorted] # Extract only the user ids.
      boundary = int(round(num_users / n)) # Floors to nearest boundary, make first tier have all extra remainder.
      list_of_tiers = []
      for i in range(0, n):
        list_of_tiers[i] = user_carbon_sorted[i*boundary:(i+1)*boundary]
      return list_of_tiers

  def computeCarbon(self, user_id, last_ts):
      """
      Computers carbon metric for specified user.
      Formula is (Actual CO2 + penalty) / distance travelled
      """
      ts = esta.TimeSeries.get_time_series(user_id)

      # Get cleaned trips for the two users that started on 1st Aug UTC
      last_period_tq = estt.TimeQuery("data.start_ts",
                          last_ts, # start of range
                          arrow.utcnow())  # end of range
      ct_df = ts.get_data_df("analysis/cleaned_trip", time_query=last_period_tq)

      carbon_val = 
      penalty_val = self.computePenalty(t_mode) # Mappings in emission/core/wrapper/motionactivity.py
      dist_travelled = ct_df[["distance"]].sum()
      return (carbon_val + penalty_val) / dist_travelled

  def computePenalty(self, t_mode):
      """
      Linear penalty functions are created depending on
      transportation mode:
      car: 50 mile threshold
      bus: 25 mile threshold
      cycling: 5 mile threshold
      """
      return

  def updateTiers(self, last_ts):
      updated_user_tiers = self.computeRanks(last_ts, 5)
      for rank in range(1, len(updated_user_tiers) + 1):
        self.addTier(rank)
        tier_users = updated_user_tiers[rank-1]
        self.tiers[rank].setUsers(tier_users)

