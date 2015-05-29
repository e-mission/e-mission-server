"""
Use this file to add all functions related to definition of socialmedia user
without polluting main api (as much as possible).
"""
import json
import facebook
from clients.socialmedia.socialmedia import FB_BASE_DIR

from dao.user import User

# TODO: May want to create an abstract parent class and/or use mixins to stay DRY.
# Seems more appropriate to have these here, but can move to the ../socialmedia.py file or to the main api
from get_database import get_uuid_db

fb_config = open(FB_BASE_DIR+'/socialmedia/settings.json')
fb_data = json.load(fb_config)


def accesstoken2UUID(args):
    pass


class FacebookUser:
    """
    Defines a new type FacebookUser dao class. Might implement to inherit or cache user object within.
    """
    def __init__(self, uuid, graph):
        self.uuid = uuid
        self.graph = graph

    @staticmethod
    def fromAccessToken(access_token):
        FacebookUser.__access_token = access_token
        graph = facebook.GraphAPI(access_token)
        email = graph.get_object('me')['email']
        email2UUID = get_uuid_db().find_one({'user_email': email})
        fbuser = FacebookUser(email2UUID['uuid'], graph)
        return fbuser


