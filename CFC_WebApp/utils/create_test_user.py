# Create a dummy user for the app store review testing
# This needs to be done every time we switch to a new database

from pymongo import MongoClient
from get_database import get_user_db
from uuid import UUID

Users=get_user_db()
fakeUser = {'user_id': 99999999999999999L,
            'uuid': UUID('{951779de-a10c-3373-b186-c1c9b14b5e38}'),
            'access_token': 'Ignore_me',
            'expires_in': 15551999,
            'token_type': 'bearer',
            'user': 'e.mission.berkeley.test@gmail.com',
            '_id': 99999999999999999L,
            'refresh_token': 'Ignore_me'}

Users.insert(fakeUser)
