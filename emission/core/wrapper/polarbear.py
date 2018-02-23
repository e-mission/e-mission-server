from emission.core.wrapper.tiersys import TierSys
from emission.core.wrapper.user import User
import emission.core.get_database as db
from uuid import UUID
import arrow


def setPolarBearattr(attrs):
	"""
	Sets a Polar Bear's attributes based on input dict:
		{user_id: num, username: string, happiness: int, oldHappiness: int, size: int}
	"""
	polarBearCollection = db.get_polarbear_db()
	userDict = polarBearCollection.find_one({'user_id' : attrs['user_id']})
	if userDict == None:
		polarBearCollection.insert_one(attrs)
	else:
		polarBearCollection.update_one(
			{'user_id': attrs['user_id']},
			{'$set' : {'username' : attrs['username'],
			'happiness' : attrs['happiness'],
			'oldHappiness': attrs['oldHappiness'],
			'size' : attrs['size']
			}}
		)

def getMoodChange(user_id):
	"""
	Returns T/F, checking if PolarBear's mood has changed
	 	relative to yesterday.
	"""
	attr = getPolarBearattr(user_id)
	happiness = attr['happiness']
	oldHappiness = attr['oldHappiness']
	attr['oldHappiness'] = happiness
	setPolarBearattr(attr)
	def checkMood(val):
		if val >= 0.6:
			return 'happy'
		elif val < 0.6 and val >= 0.4:
			return 'neutral'
		else:
			return 'sad'
	if (checkMood(happiness) == checkMood(oldHappiness)):
		return False
	return True

def getPolarBearattr(user_id):
	"""
	Return a dictionary containing all Polar Bear attributes
	 {user_id: num, username: string, happiness: int, oldHappiness: int, size: int}
	"""
	if type(user_id) == str:
		user_id = UUID(user_id)
	polarBearCollection = db.get_polarbear_db()
	return polarBearCollection.find_one({'user_id' : user_id})

def getAllBearsInTier(user_id):
	"""
	Return a dictionary containing all Polar bear attrs in a given tier
		{'myBear':{'happiness': int, 'size': int }, 'otherBears':{username1: {happiness: int, size: int},
			username2: {'happiness: int, 'size': int}...}
	"""
	if type(user_id) == str:
		user_id = UUID(user_id)
	tierSys = TierSys.getLatest()[0]
	userTier = tierSys['tiers'][TierSys.getUserTier(user_id) - 1]['users']
	myBear = getPolarBearattr(user_id)
	#List of of users within a tier
	allUsers = {'myBear': {'happiness': myBear['happiness'], 'size': myBear['size']}, 'otherBears':{}}
	for user in userTier:
		uuid = user['uuid']
		userattrs = None
		if uuid != user_id:
			userattrs = getPolarBearattr(uuid)
		if userattrs != None:
			currUsername = userattrs['username']
			allUsers['otherBears'][currUsername] = {}
			allUsers['otherBears'][currUsername]['username'] = currUsername
			allUsers['otherBears'][currUsername]['happiness'] = userattrs['happiness']
			allUsers['otherBears'][currUsername]['size'] = userattrs['size']
		else:
			print("user polar bear not found!: " + str(uuid))
	return allUsers


def updatePolarBear(user_id):
	"""
	Updates a given uuid's associated Polar Bear
	"""
	currattr = getPolarBearattr(user_id)
	if currattr == None:
		#Create a new Polar Bear for the given user
		currUsername = User.getUsername(user_id)['username']
		if currUsername == None:
			currUsername = 'Anon'
		setPolarBearattr({'user_id': user_id,
						'username': currUsername,
						'happiness': User.computeHappiness(user_id),
						'oldHappiness' : None,
						'size' : 1
						})
	else:
		#Update the user's Polar Bear with newer stats
		newHappiness = User.computeHappiness(user_id)
		currattr['oldHappiness'] = currattr['happiness']
		currattr['happiness'] = newHappiness
		currattr['username'] = User.getUsername(user_id)['username']
		rate_map = {1 : 1.05, 2: 1.03, 3: 1.012}
		if currattr['username'] == None:
			currattr['username'] = 'Anon'
		#Have to user new username if user has changed it
		if newHappiness > 0.4:
			currattr['size'] = max(currattr['size'] * rate_map[TierSys.getUserTier(user_id)], 1)
		else:
			currattr['size'] = 1
		setPolarBearattr(currattr)


def updateAll():
	"""
	Updates all Polar Bears' attributes
	"""
	tiersys = TierSys.getLatest()[0]['tiers']
	for tier in tiersys:
		for user in tier['users']:
			updatePolarBear(user['uuid'])
