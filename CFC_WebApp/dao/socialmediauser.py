"""
Use this file to add all functions related to definition of socialmedia user
without polluting main api (as much as possible).
"""
import json
from urllib import urlencode
import facebook
from dao.user import User
from get_database import get_uuid_db

# Seems more appropriate to have these here, but can move to the ../socialmedia.py file or to the main api
fb_config = open('clients/socialmedia/settings.json')
fb_data = json.load(fb_config)['client_settings']
FACEBOOK_APP_ID = fb_data['fb_app_id']
FACEBOOK_APP_SECRET = fb_data['fb_app_secret']


class FacebookUser:
    """
    Defines a new type FacebookUser dao class. Mostly a wrapper for
    creating test users as well as functional interfaces to other parts of
    facebook that we will call on regularly. The test user code was lifted and
    retrofitted from  https://github.com/tschellenbach/Django-facebook

    """
    api_url = 'https://graph.facebook.com/'
    login_url = 'https://www.facebook.com/dialog/oauth'
    access_token_url = 'https://graph.facebook.com/oauth/access_token'

    def __init__(self, uuid, graph):
        self.uuid = uuid
        self.graph = graph

    @staticmethod
    def from_access_token(access_token):
        """
        Constructor for FacebookUser from an access token. Searches the uuid_db
        for previously registered users.

        :param:  access_token: the user access token

        :return: facebook user with graph object attached. Can be used for
                    queries.
        """
        FacebookUser.__access_token = access_token
        graph = facebook.GraphAPI(access_token)
        email = graph.get_object('me')['email']
        email2UUID = get_uuid_db().find_one({'user_email': email})
        if email2UUID is None:
            User.register(email)
            email2UUID = get_uuid_db().find_one({'user_email': email})
        return FacebookUser(email2UUID['uuid'], graph)

    @staticmethod
    def get_user_from_cookies(cookies):
        """
        Retrieves the access token from a request's cookies and passes to the
        from_access_token method.

        :param cookies:

        :return: facebook user with graph object attached. Can be used for queries.
        """
        result = cookies.get('access_token')
        return FacebookUser.from_access_token(result)

    @staticmethod
    def create_test_user(app_access_token, permissions=None, name=None):
        """
        Creates a test user with the given permissions and name

        :param app_access_token:
            The application's access token

        :param permissions:
            The list of permissions to request for the test user

        :param name:
            Optionally specify the name
        """
        if not permissions:
            permissions = ['read_stream', 'publish_stream',
                           'user_photos,offline_access']
        if isinstance(permissions, list):
            permissions = ','.join(permissions)

        default_name = 'Permissions %s' % permissions.replace(
            ',', ' ').replace('_', '')
        name = name or default_name

        kwargs = {
            'access_token': app_access_token,
            'installed': True,
            'name': name,
            'method': 'post',
            'permissions': permissions,
        }
        path = '%s/accounts/test-users' % FACEBOOK_APP_ID
        # add the test user data to the test user data class
        test_user_data = facebook.GraphAPI(app_access_token).request(path, **kwargs)
        test_user_data['name'] = name
        test_user = TestUser(test_user_data)

        return test_user

    @staticmethod
    def get_or_create_test_user(app_access_token, name=None, permissions=None, force_create=False):
        """
        There is no supported way of get or creating a test user
        However
        - creating a test user takes around 5s
        - you an only create 500 test users
        So this slows your testing flow quite a bit.

        This method checks your test users
        Queries their names (stores the permissions in the name)

        """
        if not permissions:
            permissions = ['read_stream', 'publish_stream', 'publish_actions',
                           'user_photos,offline_access']
        if isinstance(permissions, list):
            permissions = ','.join(permissions)

        # hacking the permissions into the name of the test user
        default_name = 'Permissions %s' % permissions.replace(
            ',', ' ').replace('_', '')
        name = name or default_name

        # retrieve all test users
        test_users = FacebookUser.get_test_users(app_access_token)
        user_id_dict = dict([(int(u['id']), u) for u in test_users])
        user_ids = map(str, user_id_dict.keys())

        # use fql to figure out their names
        fb = facebook.GraphAPI(app_access_token)
        users = fb.fql('SELECT uid, name FROM user WHERE uid in (%s)' %
                       ','.join(user_ids))
        users_dict = dict([(u['name'], u['uid']) for u in users])
        user_id = users_dict.get(name)

        if force_create and user_id:
            # we need the users access_token, the app access token doesn't
            # always work, seems to be a bug in the Facebook api
            test_user_data = user_id_dict[user_id]
            FacebookUser.delete_test_user(test_user_data['access_token'], user_id)
            user_id = None

        if user_id:
            # we found our user, extend the data a bit
            test_user_data = user_id_dict[user_id]
            test_user_data['name'] = name
            test_user = TestUser(test_user_data)
        else:
            # create the user
            test_user = FacebookUser.create_test_user(
                app_access_token, permissions, name)

        return test_user

    @staticmethod
    def get_test_users(app_access_token):
        kwargs = dict(access_token=app_access_token)
        path = '%s/accounts/test-users' % FACEBOOK_APP_ID
        # retrieve all test users
        response = facebook.GraphAPI(app_access_token).request(path, **kwargs)
        test_users = response['data']
        return test_users

    @staticmethod
    def delete_test_user(app_access_token, test_user_id):
        kwargs = dict(access_token=app_access_token, method='delete')
        path = '%s/' % test_user_id

        # retrieve all test users
        response = facebook.GraphAPI(app_access_token).request(path, **kwargs)
        return response

    @staticmethod
    def delete_test_users(app_access_token):
        # retrieve all test users
        test_users = FacebookUser.get_test_users(app_access_token)
        test_user_ids = [u['id'] for u in test_users]
        for test_user_id in test_user_ids:
            FacebookUser.delete_test_user(app_access_token, test_user_id)

    @staticmethod
    def get_app_access_token(access_token):
        """
        Get the access_token
         for the app that can be used for
        insights and creating test users
        application_id = retrieved from the developer page
        application_secret = retrieved from the developer page
        returns the application access_token
        """
        kwargs = {
            'grant_type': 'client_credentials',
            'client_id': FACEBOOK_APP_ID,
            'client_secret': FACEBOOK_APP_SECRET,
        }
        response = facebook.GraphAPI(access_token).request('oauth/access_token', **kwargs)
        return response['access_token']

    def my_image_url(self, size='large'):
        """
            Returns the image url from your profile
            Shortcut for me/picture

            :param size:
                the type of the image to request, see facebook for available formats

            :returns: string
                url to the user's profile picture on facebook
            """
        url = '%sme/picture?%s' % (self.api_url, urlencode(size))
        return url


class TestUser(object):
    '''
    Simple wrapper around test users
    '''
    def __init__(self, data):
        self.name = data['name']
        self.id = data['id']
        self.access_token = data['access_token']
        self.data = data

    def graph(self):
        graph = facebook.GraphAPI(self.access_token)
        return graph

    def __repr__(self):
        return 'Test user %s' % self.name
