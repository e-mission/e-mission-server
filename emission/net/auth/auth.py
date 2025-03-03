from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from builtins import object
import logging
import emission.core.wrapper.user as ecwu

class AuthMethodFactory(object):
    @staticmethod
    def getAuthMethod(methodName):
        if methodName == "skip":
            import emission.net.auth.skip as enas
            logging.debug("methodName = skip, returning %s" % enas.SkipMethod)
            return enas.SkipMethod()
        if methodName == "secret":
            import emission.net.auth.secret as enar
            logging.debug("methodName = secret, returning %s" % enar.SecretMethod)
            return enar.SecretMethod()
        elif methodName == "token_list":
            import emission.net.auth.token_list as enat
            logging.debug("methodName = token_list, returning %s" % enat.TokenListMethod)
            return enat.TokenListMethod()

class AuthMethod(object):
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

def getSubgroupFromToken(token, config):
  if config is not None and "opcode" in config:
    # new style study, expects token with sub-group
    tokenParts = token.split('_');
    if len(tokenParts) <= 3:
      # no subpart defined
      raise ValueError(f"Not enough parts in {token=}, expected 3, found {tokenParts.length=}")
    if "subgroups" in config.get("opcode", {}):
      if tokenParts[2] not in config['opcode']['subgroups']:
        # subpart not in config list
        raise ValueError(f"Invalid subgroup {tokenParts[2]} not in {config.opcode.subgroups}")
      else:
        logging.debug('subgroup ' + tokenParts[2] + ' found in list ' + str(config['opcode']['subgroups']))
        return tokenParts[2];
    else:
      if tokenParts[2] != 'default':
        # subpart not in config list
        raise ValueError(f"No subgroups found in config, but subgroup {tokenParts[2]} is not 'default'")
      else:
        logging.debug("no subgroups in config, 'default' subgroup found in token ");
        return tokenParts[2];
  else:
    # old style study, expect token without subgroup
    # nothing further to validate at this point
    # only validation required is `nrelop_` and valid study name
    # first is already handled in getStudyNameFromToken, second is handled
    # by default since download will fail if it is invalid
    logging.debug('Old-style study, expecting token without a subgroup...');
    return None;

def getUUID(request, authMethod, inHeader=False, dynamicConfig = None):
  retUUID = None
  userToken = __getToken__(request, inHeader)
  curr_subgroup = getSubgroupFromToken(userToken, dynamicConfig)
  retUUID = getUUIDFromToken(authMethod, userToken)
  request.params.user_uuid = retUUID
  # TODO: We should really think about how long we want to continue supporting
  # non-dynamic config. Check with community on who's using it and what they need.
  if dynamicConfig is not None:
    subgroup = getSubgroupFromToken(userToken, dynamicConfig)
    return {"subgroup": subgroup, "user_id": retUUID}
  else:
    # if the authmethod is "skip" or "token_list", the token can be in any format
    return retUUID

# Should only be used by the profile creation code, since we may not have a
# UUID yet. All others should only use the UUID.
def _getEmail(request, authMethod, inHeader=False):
  userToken = request.json['user']
  # This is the only place we should use the email, since we may not have a
  # UUID yet. All others should only use the UUID.
  userEmail = AuthMethodFactory.getAuthMethod(authMethod).verifyUserToken(userToken)
  return userEmail
