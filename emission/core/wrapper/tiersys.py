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
        return get_tiersys_db().find_one({'newUserTier' : 4})
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
        uuids = [user['uuid'] for user in newUserTier['users']]
        if user_id in uuids:
            return 4
        return None

    @staticmethod
    def computeTierRank(user_id):
        tierSys = TierSys.getLatest()[0]
        #allTiers- dict rankNum : tierObject
        userTier = tierSys['tiers'][TierSys.getUserTier(user_id) - 1]['users']
        uuids = [user['uuid'] for user in userTier]
        #uuids - list of UUIDs of users within tier
        userCarbon, userRank = {}, {}
        # userCarbon- dict HappinessMetric : UUID
        # userRank- dict UUID : Position
        for uuid in uuids:
            userCarbon[User.computeHappiness(user)] = uuid
        sortedCarbonVals = list(userCarbon.keys())
        sortedCarbonVals.sort()
        for carbon, pos in zip(sortedCarbonVals, range(len(userCarbon))):
            userRank[userCarbon[carbon]] = len(userCarbon) - pos
        return userRank[user_id]


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
        avg = len(seq) / float(num)
        out = []
        last = 0.0
        while last < len(seq):
            out.append(seq[int(last):int(last + avg)])
            last += avg
        return out

    @staticmethod
    def addUser(user_id):
        '''
        Adds a user to the new people tier.
            Used upon study start.
        '''
        if type(user_id) == str:
            user_id = UUID(user_id)
        tierSys = TierSys.getNewUserTier()
        newUser = {'uuid': user_id, "lastWeekCarbon": 0.0}
        if tierSys == None:
            newTiers = [newUser]
            get_tiersys_db().insert_one({'newUserTier': 4}, {'users': newTiers})
        else:
            tierSys['users'].append(newUser)
            newTiers = tierSys['tiers']
            get_tiersys_db().update_one({'newUserTier': 4},
                {'$set': {'users': newTiers}
                }
            )

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
                user_carbon_map[user_id] = val
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
        for i in range(len(self.tiers)):
            users = [{'uuid': uuid, 'lastWeekCarbon': User.computeCarbon(uuid, last_ts, curr_ts)} for uuid in self.tiers[i]]
            ts.append({'rank': i + 1, 'users': users})

        logging.debug(ts)

        get_tiersys_db().insert_one({'tiers': ts, 'created_at': datetime.now()})
        get_tiersys_db().update_one({'newUserTier' : 4}, {'$set': {'users': []}})
        return ts

def m_to_km(distance):
    return max(0, float(distance) / float(1000))

def mil_to_km(miles):
    return float(miles) * 1609.344/float(1000)
