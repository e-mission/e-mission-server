from emission.core.wrapper.tiersys import Tiersys
from emission.core.wrapper.user import User
import emission.core.get_database as db


def setPolarBearattr(attrs):
	"""
	Sets a Polar Bear's attributes based on input dict:
		{user_id: num, username: string, happiness:int, size: int}
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
			'size' : attrs['size']
			}}
		)

def getPolarBearattr(user_id):
	"""
	Return a dictionary containing all Polar Bear attributes
	 {user_id: num, username: string, happiness:int, size: int}
	"""
	polarBearCollection = db.get_polarbear_db()
	return polarBearCollection.find_one({'user_id' : user_id})

def getAllBearsInTier(user_id):
	"""
	Return a dictionary containing all Polar bear attrs in a given tier
		{'username1':[happiness1, size1], 'username2':[happiness2, size2]...}
	"""
	tiersys = Tiersys.getLatest()[0]
	userTier = tierSys['tiers'][TierSys.getUserTier(user_id) - 1]['uuids']
	#List of uuids of users within a tier
	allUsers = {}
	for uuid in userTier:
		userattrs = getPolarBearattr(uuid)
		currUsername = userattrs['username']
		allUsers[currUsername] = [userattrs['happiness'], userattrs['size']]
	return allUsers


def updatePolarBear(user_id):
	"""
	Updates a given uuid's associated Polar Bear
	"""
	currattr = getPolarBearattr(user_id)
	if currattr == None:
		#Create a new Polar Bear for the given user
		currUsername = User.getUsername(user_id)
		if currUsername == None:
			currUsername = 'Anon'
		setPolarBearattr({'user_id': user_id,
						'username': currUsername,
						'happiness': User.computeHappiness(user_id),
						'size' : 0
						})
	else:
		#Update the user's Polar Bear with newer stats
		newHappiness = User.computeHappiness(user_id)
		currattr['happiness'] = newHappiness
		currattr['username'] = User.getUsername(user_id)
		if currattr['username'] == None:
			currattr['username'] = 'Anon'
		#Have to user new username if user has changed it
		if newHappiness > 0.4:
			currattr['size'] += (4 - Tiersys.getUserTier(user_id))
		else:
			currattr['size'] = 0
		setPolarBearattr(currattr)


def updateAll():
	"""
	Updates all Polar Bears' attributes
	"""
	tiersys = Tiersys.getLatest()[0]['tiers']
	for tier in tiersys:
		for user_id in tier['uuids']:
			updatePolarBear(user_id)
