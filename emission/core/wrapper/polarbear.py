from emission.core.wrapper.tiersys import TierSys
from emission.core.wrapper.user import User
import emission.core.get_database as db
from uuid import UUID
import arrow
import logging
import logging.config
from random import shuffle



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
	attrs = polarBearCollection.find_one({'user_id' : user_id})
	if attrs is not None:
		if "Anon" in attrs['username']:
			if User.getUsername(user_id) is not None:
				polarBearCollection.update_one({'user_id': attrs['user_id']},{'$set' : {'username' : User.getUsername(user_id),
		                                                                        'happiness' : attrs['happiness'],
		                                                                        'oldHappiness': attrs['oldHappiness'],
		                                                                        'size' : attrs['size']}})
			attrs = polarBearCollection.find_one({'user_id' : user_id})
	return attrs

def getAllBearsInTier(user_id):
	"""
	Return a dictionary containing all Polar bear attrs in a given tier
		{'myBear':{'happiness': int, 'size': int }, 'otherBears':{username1: {happiness: int, size: int},
			username2: {'happiness: int, 'size': int}...}
	Limited to 4 random polar bears
	"""
	if type(user_id) == str:
		user_id = UUID(user_id)
	tierNum = TierSys.getUserTier(user_id)
	if tierNum != 4 and tierNum != None:
		tierSys = TierSys.getLatest()[0]
		userTier = tierSys['tiers'][tierNum - 1]['users']
	else:
		tierSys = TierSys.getNewUserTier()[0]
		userTier = tierSys['users']
	myBear = getPolarBearattr(user_id)
	if myBear == None:
		return None
	#List of of users within a tier
	allUsers = {'myBear': {'happiness': myBear['happiness'], 'size': myBear['size']}, 'otherBears':{}}
	logging.debug("User tier contains: %s" %userTier)
	if userTier is None:
		return allUsers
	counter = 5
	shuffle(userTier)
	for user in userTier:
		if counter == 0:
			break
		counter -= 1
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

def assignName(user_id):
	if type(user_id) == str:
		user_id = UUID(user_id)
	tierNum = TierSys.getUserTier(user_id)
	if tierNum != 4 and tierNum != None:
		tierSys = TierSys.getLatest()[0]
		userTier = tierSys['tiers'][tierNum - 1]['users']
	else:
		tierSys = TierSys.getNewUserTier()[0]
		userTier = tierSys['users']
	allPolarUsernames = []
	for user in userTier:
		currID = user['uuid']
		currPolarAttr = getPolarBearattr(currID)
		if currPolarAttr != None:
			allPolarUsernames.append(currPolarAttr['username'])
	newUsername = "Anon"
	count = 1
	while newUsername in allPolarUsernames:
		newUsername = "Anon" + str(count)
		count += 1
	return newUsername

def updatePolarBear(user_id):
	"""
	Updates a given uuid's associated Polar Bear
	"""
	currattr = getPolarBearattr(user_id)
	if currattr == None:
		#Create a new Polar Bear for the given user
		currUsername = User.getUsername(user_id)
		if currUsername == None:
			currUsername = assignName(user_id)
		else:
			currUsername = currUsername['username']
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

	uname = User.getUsername(user_id)
	if currattr != None and uname == None:
		currattr['username'] = assignName(user_id)
	elif currattr != None:
		currattr['username'] = uname['username']
		if currattr['username'] == None:
			currattr['username'] = assignName(user_id)

		rate_map = {1 : 0.15, 2: 0.1, 3: 0.05, 4: 0}
		#Have to user new username if user has changed it
		if newHappiness > 0.4:
			currattr['size'] = currattr['size'] + rate_map[TierSys.getUserTier(user_id)]
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
	newUsers = TierSys.getNewUserTier()[0]['users']
	for user in newUsers:
		updatePolarBear(user['uuid'])
