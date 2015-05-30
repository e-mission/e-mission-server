import unittest
from dao.SocialMediaUser import FacebookUser
from get_database import get_db


class TestSocialMedia(unittest.TestCase):
    def setUp(self):
        from tests.common import dropAllCollections
        dropAllCollections(get_db())
        user = FacebookUser.register("oqsaqjk_adeagbosky_1432921786@tfbnw.net")
        self.uuid = user.uuid


if __name__ == '__main__':
    unittest.main()