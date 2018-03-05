import emission.core.wrapper.tier as st
import emission.core.get_database as edb
import pandas as pd
from uuid import UUID
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt
import emission.core.wrapper.motionactivity as ecwm
from emission.core.wrapper.user import User
import arrow
from emission.core.get_database import get_tiersys_db
from emission.core.get_database import get_new_tier_db
from emission.core.get_database import get_carbon_usage_db
import logging
import logging.config

class TierSys:
    def __init__(self, num_tiers=3):
        self.tiers = []
        for i in range(1, num_tiers+1):
           self.addTier(i)

    @staticmethod
    def getLatest():
        return get_tiersys_db().find().sort('created_at',-1).limit(1)

    @staticmethod
    def getNewUserTier():
        return get_new_tier_db().find().sort('created_at', -1).limit(1)

    @staticmethod
    def getUserTier(user_id):
        if type(user_id) == str:
            user_id = UUID(user_id)
        allTiers = TierSys.getLatest()[0]
        index = 1
        for tier in allTiers['tiers']:
            uuids = [user['uuid'] for user in tier['users']]
            if user_id in uuids:
                return index
            else:
                index += 1
        newUserTier = TierSys.getNewUserTier()[0]
        if newUserTier is None:
            return 4
        elif newUserTier['users'] is None:
            return 4
        uuids = [user['uuid'] for user in newUserTier['users']]
        if user_id in uuids:
            return 4
        return None

    @staticmethod
    def computeTierRank(user_id):
        if type(user_id) == str:
                    user_id = UUID(user_id)
        tierSys = TierSys.getLatest()[0]
        #allTiers- dict rankNum : tierObject
        tierNum = TierSys.getUserTier(user_id)
        if tierNum == 4:
            return 1
        userTier = tierSys['tiers'][tierNum - 1]['users']
        uuids = [user['uuid'] for user in userTier]
        #uuids - list of UUIDs of users within tier
        userCarbon, userRank = {}, {}
        userCarbon = []
        # userCarbon- dict HappinessMetric : UUID
        # userRank- dict UUID : Position
        for uuid in uuids:
            currHappiness = User.computeHappiness(uuid)
            userCarbon.append((currHappiness, uuid))
        sorted(userCarbon, key = lambda x: x[0])
        for currUser, pos in zip(userCarbon, range(1, len(userCarbon) + 1)):
            if currUser[1] == user_id:
                return pos


    def getAllTiers(self):
        return self.tiers

    def addTier(self, rank):
        self.tiers.append([])

    """def deleteTier(self, rank):
        if rank in self.tiers:
            self.tiers.pop(rank)
        else:
            raise Exception('Inputted rank does not exist in the tiersys.')"""

    @staticmethod
    def divideIntoBuckets(seq, num):
        """
        Divides objects into n buckets.
        Used in compute ranks to divide users into n tiers
        """
        n, k = len(seq), num
        return [seq[i * (n // k) + min(i, n % k):(i+1) * (n // k) + min(i+1, n % k)] for i in range(k)]

    @staticmethod
    def addUser(user_id):
        from datetime import datetime
        '''
        Adds a user to the new people tier.
            Used upon study start.
        '''
        if type(user_id) == str:
            user_id = UUID(user_id)
        newTierCollection = get_new_tier_db()
        newUser = {'uuid': user_id, "lastWeekCarbon": 0.0}
        newTier = TierSys.getNewUserTier()[0]
        if newTier is None:
            updatedUsers = []
            updatedUsers.append(newUser)
            newTierCollection.insert_one({'users': updatedUsers, 'created_at': datetime.now()})
        else:
            allUsers = newTier['users']
            if allUsers is None:
                updatedUsers = []
                updatedUsers.append(newUser)
                newTierCollection.insert_one(
                {'users': updatedUsers, 'created_at': datetime.now()})
            else:
                allUsers.append(newUser)
                newTierCollection.insert_one(
                {'users': allUsers, 'created_at': datetime.now()})

    def computeRanks(self, last_ts, n):
        #TODO: FINISH
        """
        Get all users from DB
        sort them by carbon metric
        compute percentiles
        set boundaries
        return list of n lists for n ranks,
        with the tiers sorted in ascending order.
        (lower carbon val comes first)
        """
        user_carbon_map = {} # Map from user_id to carbon val.
        all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"uuid": 1, "_id": 0})))
        num_users = all_users.shape[0]
        if num_users <= 0:
            raise Exception("No users in DB")
        for index, row in all_users.iterrows():
            user_id = row['uuid']
            val = self.computeCarbon(user_id, last_ts)
            if val != None:
                try:
                    client = edb.get_profile_db().find_one({"user_id": user_id})['client']
                    user_carbon_map[user_id] = val
                except:
                    logging.debug("Failed to get client in TierSys")
                    continue
        logging.debug('USER CARBON MAP')
        logging.debug(user_carbon_map)
        # Sort and partition users by carbon metric.
        user_carbon_tuples_sorted = sorted(user_carbon_map.items(), key=lambda kv: kv[1]) # Sorted list by value of dict tuples.
        logging.debug('USER CARBON TUPLES SORTED')
        logging.debug(user_carbon_tuples_sorted)
        user_carbon_sorted = [i[0] for i in user_carbon_tuples_sorted] # Extract only the user ids.
        return self.divideIntoBuckets(user_carbon_sorted, n)

    def computeCarbon(self, user_id, last_ts):
        """
        Computers carbon metric for specified user.
        Formula is (Actual CO2 + penalty) / distance travelled
        """
        curr_ts = arrow.utcnow().timestamp
        return User.computeCarbon(user_id, last_ts, curr_ts)

    def updateTiers(self, last_ts):
        """
        Resets user tiers for the last timestep to the present.
        Also updates users tier attributes in the database.
        "Best" tiers have lower rank values.
        """
        self.tiers = []
        updated_user_tiers = self.computeRanks(last_ts, 3)

        for rank in range(1, len(updated_user_tiers) + 1):
            self.addTier(rank)
            tier_users = updated_user_tiers[rank-1]
            self.tiers[rank - 1] = tier_users
        return self.tiers

    def saveTiers(self, last_ts):
        from datetime import datetime
        """
        Saves the current tiers into the tiersys.
        Adds/Replaces array of tiers into tiersys object
        Gets current array of uuids and puts them into tier objects
        {{
            _id : DEFINED BY MONGO,
            created_at: datetime,
            tiers : [{
                rank : 0,
                users : [
                    {
                    uuid: 1234,
                    lastWeekCarbon: 1.2 g/mile
                    } , ...
                ]
            }, {
                rank : 1,
                users : [
                    {
                    uuid: 1234,
                    lastWeekCarbon: 1.2 g/mile
                    } , ...
                ]
            }]
        }}
        """
        curr_ts = arrow.utcnow().timestamp
        ts = []
        carbonDB = {'timestamp': curr_ts, 'users': []}
        for i in range(len(self.tiers)):
            users = []
            for uuid in self.tiers[i]:
                client = edb.get_profile_db().find_one({"user_id": uuid})['client']
                '''
                carbonLWU -> Carbon Last Week Unpenalized (normalized with distance),
                carbonLWUR -> Carbon Last Week Unpenalized Raw (not normalized with distance),
                carbonLWP -> Carbon Last Week Penalized
                '''
                userCarbonRaw = User.computeCarbonRaw(uuid, last_ts, curr_ts)
                carbonLWP = User.computeCarbon(uuid, last_ts, curr_ts)
                carbonLWU = None
                carbonLWUR = None

                if userCarbonRaw != None:
                    carbonLWU = userCarbonRaw[0]
                    carbonLWUR = userCarbonRaw[1]

                userStats = {'user_id': uuid, 'tier': i + 1, 'client': client, "carbonLWU": carbonLWU,
                                'carbonLWP': carbonLWP, 'carbonLWUR': carbonLWUR}

                carbonDB['users'].append(userStats)
                users.append({'uuid': uuid, 'lastWeekCarbon': carbonLWP})
            ts.append({'rank': i + 1, 'users': users})

            #users = [{'uuid': uuid, 'lastWeekCarbon': User.computeCarbon(uuid, last_ts, curr_ts)} for uuid in self.tiers[i]]
            #ts.append({'rank': i + 1, 'users': users})

        logging.debug(ts)
        get_carbon_usage_db().insert_one(carbonDB)
        get_tiersys_db().insert_one({'tiers': ts, 'created_at': datetime.now()})
        get_new_tier_db().insert_one({'users': [], 'created_at': datetime.now()})
        return ts

def m_to_km(distance):
    return max(0, float(distance) / float(1000))

def mil_to_km(miles):
    return float(miles) * 1609.344/float(1000)
