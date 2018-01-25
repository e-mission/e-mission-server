class Tier:
  def __init__(self, tid, users = []):
    self.tid = tid
    self.users = users

  def getTierUsers(self):
  	return self.users

  def getUsers(self):
    return self.uuids

  def setUsers(self, users):
    self.uuids = users