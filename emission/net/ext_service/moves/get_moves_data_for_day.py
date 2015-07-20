import json
import sys
import os

from emission.net.ext_services.moves.sdk import Moves

# It is hard, if not impossible to test this properly.
# First, we would need to write an integration test, not a unit test
# Second, the test would need to hardcode a token, which expires every 6 months,
# so the test would need to be refreshed every 6 months
# Third, the full storyline with trackpoints is only available for 7 days, so the test
# would need to call the service with a dynamic date, which means that we cannot check in
# the expected result.
# 
# The traditional solution would be to provide a mock class to test against.
# However, given that this is not part of the normal operation of the system,
# and is a debug tool anyway, I am just going to skip the unit test

def getMovesDataForDay(token, day, fileName):
  ''' 
    The userName is the OAuth2 email address (userName).
    The token has to be obtained in advance from the moves database.
    The day is in the format YYYYMMDD (ie 20140303)
  '''

  key_file = open('keys.json')
  key_data = json.load(key_file)
  m = Moves(client_id = key_data["moves"]["client_id"],
            client_secret = key_data["moves"]["client_secret"],
            redirect_url = key_data["moves"]["https_redirect_url"])
  endPoint = "user/storyline/daily/%s?trackPoints=true" % day
  result = m.get(token=token, endpoint = endPoint)
  fh = open(fileName, "w")
  json.dump(result, fh, indent=4)
  fh.flush()
  fh.close()

if __name__ == "__main__":
  getMovesDataForDay(sys.argv[1], sys.argv[2],  sys.argv[3])
