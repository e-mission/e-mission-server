import logging
import unittest
from dao.socialmediauser import FacebookUser
from dao.user import User
from get_database import get_db

logging.basicConfig(level=logging.DEBUG)

class TestSocialMedia(unittest.TestCase):
    def setUp(self):
        from tests.common import dropAllCollections
        dropAllCollections(get_db())
        user = User.register("oqsaqjk_adeagbosky_1432921786@tfbnw.net")
        self.uuid = user.uuid




if __name__ == '__main__':
    unittest.main()