# A python class for easy access to the Moves App data. Created by Joost Plattel [http://github.com/jplattel]

import requests

class Moves():
  def __init__(self, client_id, client_secret, redirect_url, \
        auth_url = 'https://api.moves-app.com/oauth/v1/', \
        api_url = 'https://api.moves-app.com/api/1.1/'):
      self.client_id = client_id	   # Client ID, get this by creating an app
      self.client_secret = client_secret # Client Secret, get this by creating an app
      self.redirect_url = redirect_url  # Callback URL for getting an access token
      self.api_url = api_url

	# Generate an request URL
  def request_url(self):
      u = 'https://api.moves-app.com/oauth/v1/authorize?response_type=code'
      c = '&client_id=' + self.client_id
      s = '&scope=' + 'activity location' # Assuming we want both activity and locations
      url = u + c + s
      return url # Open this URL for the PIN, then authenticate with it and it will redirect you to the callback URL with a request-code, specified in the API access.

	# Get access_token 
  def auth(self, request_token):
    c = '&client_id=' + self.client_id
    r = '&redirect_uri=' + self.redirect_url
    s = '&client_secret=' + self.client_secret
    j = requests.post(self.auth_url + 'access_token?grant_type=authorization_code&code=' + request_token + c + s + r)
    self.access_json = j.json()
    token = self.access_json['access_token']
    return token 
		
	# Standard GET and profile requests

	# Base request
  def get(self, token, endpoint):
    if ('?' in endpoint):
      tokenSep = '&'
    else:
      tokenSep = '?'
    token = tokenSep + 'access_token=' + token
    response = requests.get(self.api_url + endpoint + token)
    try:
      response.raise_for_status();
      return response.json()
    except requests.exceptions.HTTPError as e:
      print "Got HTTP error %s " % e
      return []
    except:
      print "Unable to decode response %s with code %s" % (response.text, response.status_code)
      return []

	# /user/profile
  def get_profile(self, token):
      token = '?access_token=' + token
      root = 'user/profile'
      response = requests.get(self.api_url + root + token)
      try:
      	return response.json()
      except:
        print "Unable to decode response %s" % response.text
        return None

	# Summary requests

	# /user/summary/daily/<date>
	# /user/summary/daily/<week>
	# /user/summary/daily/<month>
  def get_summary(self, token, date):
      token = '?access_token=' + token
      return requests.get(self.api_url + '/user/summary' + date + token).json()

	
	# Range requests, max range of 7 days!

	# /user/summary/daily?from=<start>&to=<end>
	# /user/activities/daily?from=<start>&to=<end>
	# /user/places/daily?from=<start>&to=<end>
	# /user/storyline/daily?from=<start>&to=<end>
  def get_range(self, access_token, endpoint, start, end):
      export = get(access_token, endpoint + '?from=' + start + '&to=' + end)
      return export


