class TierSys:
  def __init__(self, tsid):
    self.tsid = tsid
    self.tiers = {}
    for i in range(1, 6):
      self.addTier(i)

  def updateTiers(self):
    
  def addTier(self, rank):
      self.tiers[rank] = Tier(rank)

  def computeRanks(self, n):
    #TODO: FINISH
      """get all users from DB
      sort them by carbon metric
      compute percentiles
      set boundaries
      return list of n lists for n ranks"""
      ret = []
      num_users = #GET NUM USERS
      boundary = int(round(num_users / n)) #rounds down to nearest boundary, make first tier have all extra remainder
  def computeCarbon(self, uuid):
      """
      car: 50 mile threshold
      bus: 25 mile threshold
      cycling: 5 mile threshold
      idea: we need to go through all trips
      sinc 5 days ago and
      compute each carbon metric for 
      each trip, aggregate carbon metric
      values to get their total penalty
      """
