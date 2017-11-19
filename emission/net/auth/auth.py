import logging
import emission.core.wrapper.user as ecwu

class AuthMethodFactory:
    @staticmethod
    def getAuthMethod(methodName):
        if methodName == "skip":
            import emission.net.auth.skip as enas
            logging.debug("methodName = skip, returning %s" % enas.SkipMethod)
            return enas.SkipMethod()
        elif methodName == "google_auth":
            import emission.net.auth.google_auth as enag
            logging.debug("methodName = google_auth, returning %s" % enag.GoogleAuthMethod)
            return enag.GoogleAuthMethod()
        elif methodName == "token_list":
            import emission.net.auth.token_list as enat
            logging.debug("methodName = token_list, returning %s" % enat.TokenListMethod)
            return enat.TokenListMethod()

class AuthMethod:
    def verifyUserToken(self, token):
        raise NotImplementedError('call to abstract method verifyUserToken')

def getUUIDFromToken(authMethod, token):
    userEmail = AuthMethodFactory.getAuthMethod(authMethod).verifyUserToken(token)
    return __getUUIDFromEmail__(userEmail)

# This should not be used for general API calls
def __getUUIDFromEmail__(userEmail):
    user=ecwu.User.fromEmail(userEmail)
    if user is None:
        return None
    user_uuid=user.uuid
    return user_uuid

def __getToken__(request, inHeader):
    if inHeader:
      userHeaderSplitList = request.headers.get('User').split()
      if len(userHeaderSplitList) == 1:
          userToken = userHeaderSplitList[0]
      else:
          userToken = userHeaderSplitList[1]
    else:
      userToken = request.json['user']

    return userToken

def getUUID(request, authMethod, inHeader=False):
  retUUID = None
  userToken = __getToken__(request, inHeader)
  retUUID = getUUIDFromToken(authMethod, userToken)
  request.params.user_uuid = retUUID
  return retUUID

# Should only be used by the profile creation code, since we may not have a
# UUID yet. All others should only use the UUID.
def __getEmail(request, authMethod, inHeader=False):
  userToken = request.json['user']
  # This is the only place we should use the email, since we may not have a
  # UUID yet. All others should only use the UUID.
  userEmail = AuthMethodFactory.getAuthMethod(authMethod).verifyUserToken(userToken)
  return userEmail
