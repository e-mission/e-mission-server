# When I initially started tracking scores, I was lazy and stored the
# carbonFootprint in the same field as the gamification score, instead of
# having client specific fields. This made sense if you thought that there was
# one "result" field that was populated in different ways by different clients.
# Unfortunately, this model broke for the "choice" client, because we wanted to
# store multiple results at the same time.
# So now, I have moved the field access code into the specific clients.
# But I need to go through and fix the existing users
# For all users who are not in the "gamified" group, we need to move their
# carbon footprint from the currentScore field to the carbon_footprint field,
# and delete the currentScore and previousScore fields

from get_database import get_uuid_db, get_profile_db
from dao.user import User
from clients.default import default
import logging

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    for user_uuid_dict in get_uuid_db().find({}, {'uuid': 1, '_id': 0}):
        currUUID = user_uuid_dict['uuid']
        logging.info("Fixing results for %s" % currUUID)
        currUser = User.fromUUID(currUUID)
        if currUser.getFirstStudy() is None:
            currFootprint = currUser.getProfile().get("currentScore", None)
            default.setCarbonFootprint(currUser, currFootprint)
            get_profile_db().update({'user_id': currUUID}, {'$unset': {'previousScore': "",
                                                                        'currentScore': ""}})
            logging.debug("After change, currentScore = %s, currFootprint = %s" % (
                currUser.getProfile().get("currentScore"),
                default.getCarbonFootprint(currUser)))

# Informal testing from the command line since this is a one-time script
# Can be pulled out into a unit test if reworked
# Test setup steps from the REPL:
# In [52]: userTest = User.register("test.unset@gmail.com")
# In [53]: userTest1 = User.register("test.unset1@gmail.com")
# In [54]: gamified.setScores(userTest, None, [{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}])
# In [55]: gamified.setScores(userTest1, None, [{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}])
