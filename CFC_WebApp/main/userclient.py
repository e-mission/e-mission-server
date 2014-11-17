# Functions that manipulate both user and client information. It seemed better
# to call that out into a separate module, rather than decide whether to stick
# it into User or into Client.

def getClientSpecificQueryFilter(user_uuid):
    from dao.user import User
    from dao.client import Client

    studyList = User.fromUUID(user_uuid).getStudy()
    if len(studyList) > 0:
      assert(len(studyList) == 1)
      study = studyList[0]
      client = Client(study)
      return client.getSectionFilter(user_uuid)
    else:
      # User is not part of any study, so no additional filtering is needed
      return []

