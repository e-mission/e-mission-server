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
      ret = []
      num_users = #GET NUM USERS
      boundary = int(round(num_users / n)) #rounds down to nearest boundary, make first tier have all extra remainder
  
  def computeCarbon(self, uuid, last_ts):
      """
      Computers carbon metric for specified user.
      Formula is (Actual CO2 + penalty) / distance travelled
      """
      carbon_val = 
      penalty_val = self.computePenalty(t_mode)
      dist_travelled = 
      return (carbon_val + penalty_val) / dist_travelled

  def computePenalty(self, t_mode):
      """
      Linear penalty functions are created depending on
      transportation mode:
      car: 50 mile threshold
      bus: 25 mile threshold
      cycling: 5 mile threshold
      idea: we need to go through all trips
      sinc 5 days ago and
      compute each carbon metric for 
      each trip, aggregate carbon metric
      values to get their total penalty
      """

  def updateTiers(self, last_ts):
      updated_user_tiers = self.computeRanks(last_ts, 5)
      for rank in range(1, len(updated_user_tiers) + 1):
        self.addTier(rank)
        tier_users = updated_user_tiers[rank-1]
        self.tiers[rank].setUsers(tier_users)

