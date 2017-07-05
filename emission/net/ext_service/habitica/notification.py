# Standard imports
import requests
import logging

# Our imports
import emission.net.ext_service.push.notify_usage as pnu

def notify_quest_invitation(user_id_list):
	# Get each user's habitica object 
	method_url_get_user = '/api/v3/user'
	method_url_get_party = '/api/v3/groups/party'
	users_RSVPNeeded = {}

	for user_id in user_id_list:
		res = proxy.habiticaProxy(user_id, 'GET', method_url_get_user, None)
		logging.debug("Request to get user %s" % res)
		user = res.data

		# Add users and thier party name to list with pending quest invitation
		if(user.party.quest.RSVPNeeded):
			party_res = proxy.habiticaProxy(user_id, 'GET', method_url_get_party, None)
			party_name = party_res.data.name
			users_RSVPNeeded[user_id] = party_name

	# Title for pending quest notification
	# Message an json data is empty to keep notification short
	message = ""
	json_data = {}

	for user_id in users_RSVPNeeded:
		title = users_RSVPNeeded[user_id] " sent a quest invitation"
		# notification method takes user_id list 
		user_id_param = [user_id]
		send_visible_notification_to_users(user_id_param, title, message, json_data)






