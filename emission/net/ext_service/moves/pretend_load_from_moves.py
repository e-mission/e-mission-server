# Standard imports
import unittest
import json

# Our imports
import emission.net.ext_services.moves.register as auth
from emission.net.ext_services.moves import collect

sampleAuthMessage = {'user_id': 99999999999999999, 'access_token': 'Ignore_me', 'expires_in': 15551999, 'token_type': 'bearer', 'refresh_token': 'Ignore_me'}

def loadMovesInputFile(userEmail, fileName):
  from dao.user import User

  user = User.fromEmail(userEmail)
  savedTokens = auth.getAccessToken(user.uuid)
  print savedTokens
  if len(savedTokens) == 0:
    auth.saveAccessToken(sampleAuthMessage, user.uuid)
  result = json.load(open(fileName))
  print json.dumps(result)
  collect.processResult(user.uuid, result)  

if __name__ == '__main__':
    loadMovesInputFile(sys.argv[1], sys.argv[2])
