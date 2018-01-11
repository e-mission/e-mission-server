import emission.core.wrapper.tier as st
import emission.core.get_database as edb
import pandas as pd
from uuid import UUID
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt
import emission.core.wrapper.motionactivity as ecwm
import arrow
from emission.core.get_database import get_tiersys_db

class TierSys:
    def __init__(self, num_tiers=5):
        self.tiers = {}
        for i in range(1, num_tiers+1):
           self.addTier(i)

    @staticmethod
    def getLatest():
        return get_tiersys_db().find().sort('created_at',-1).limit(1)

    def addTier(self, rank):
        if rank not in self.tiers:
            self.tiers[rank] = st.Tier(rank)
        else:
            raise Exception('Inputted rank already exist in the tiersys.')

    def deleteTier(self, rank):
        if rank in self.tiers:
            self.tiers.pop(rank)
        else:
            raise Exception('Inputted rank does not exist in the tiersys.')

    def computeRanks(self, last_ts, n):
        #TODO: FINISH
        """
        Get all users from DB
        sort them by carbon metric
        compute percentiles
        set boundaries
        return list of n lists for n ranks,
        with the tiers sorted in descending order.
        """
        user_carbon_map = {} # Map from user_id to carbon val.
        num_users = len(all_users.iloc)
        all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"user_email":1, "uuid": 1, "_id": 0})))
        for i in range(1, num_users+1):
            user_id = all_users.iloc[i].UUID
            user_carbon_map[user_id] = self.computeCarbon(user_id, last_ts)

        # Sort and partition users by carbon metric.
        user_carbon_tuples_sorted = sorted(user_carbon_map.iteritems(), key=(lambda kv: (kv[1],kv[0]))) # Sorted list by value of dict tuples.
        user_carbon_sorted = [i[0] for i in user_carbon_tuples_sorted] # Extract only the user ids.
        boundary = int(round(num_users / n)) # Floors to nearest boundary, make first tier have all extra remainder.
        list_of_tiers = []
        for i in range(0, n):
            list_of_tiers[i] = user_carbon_sorted[i*boundary:(i+1)*boundary]
        return list_of_tiers

    def computeCarbon(self, user_id, last_ts):
        """
        Computers carbon metric for specified user.
        Formula is (Actual CO2 + penalty) / distance travelled
        """
        ts = esta.TimeSeries.get_time_series(user_id)

        # Get cleaned trips for the two users that started on 1st Aug UTC
        last_period_tq = estt.TimeQuery("data.start_ts",
                            last_ts, # start of range
                            arrow.utcnow())  # end of range
        cs_df = ts.get_data_df("analysis/cleaned_section", time_query=last_period_tq)

        carbon_val = self.computeFootprint(cs_df[["sensed_mode", "distance"]])
        penalty_val = self.computePenalty(cs_df[["sensed_mode", "distance"]]) # Mappings in emission/core/wrapper/motionactivity.py
        dist_travelled = cs_df[["distance"]].sum()

        if dist_travelled > 0:
            return (carbon_val + penalty_val) / dist_travelled
        # Do not include no distance traveled users in the tier system.
        return None

    def computeFootprint(self, footprint_df):
        """
        Inspired by e-mission-phone/www/js/metrics-factory.js

        train: 92/1609,
        car: 287/1609,
        ON_FOOT/BICYCLING: 0

        Computes range and for now calculates the average since we
        don't distinguish between train and car.

        If unknown (sensed mode = 4), don't compute anything for now.

        footprint_df: [[trip1mode, distance], [trip2mode, distance], ...]

        """
        fp_train = 92.0/1609.0
        fp_car = 287.0/1609.0
        total_footprint = 0
        for index, row in footprint_df.iterrows():
            motiontype = int(row['sensed_mode'])
            distance = row['distance']
            if motiontype == ecwm.MotionTypes.IN_VEHICLE.value:
                # TODO: Replace this avg with public transportation/car value when we can distinguish.
                total_footprint += (fp_train * m_to_km(distance) + fp_car * m_to_km(distance)) / 2;
        return total_footprint

    def computePenalty(self, penalty_df):
        """
        Linear penalty functions are created depending on
        transportation mode:
        car: 50 mile threshold, penalty = 50 - distance
        bus: 25 mile threshold, penalty = 25 - distance

        If unknown (sensed mode = 4), don't compute anything for now.

        penalty_df: [[trip1mode, distance], [trip2mode, distance], ...]

        """
        #TODO: Differentiate between car and bus, check ML & try to add to ecwm.MotionTypes...
        total_penalty = 0
        for index, row in penalty_df.iterrows():
            motiontype = int(row['sensed_mode'])
            if motiontype == ecwm.MotionTypes.IN_VEHICLE.value:
                total_penalty += max(0, 37.5 - row['distance'])
        return total_penalty

    def updateTiers(self, last_ts):
        """
        Resets user tiers for the last timestep to the present.
        Also updates users tier attributes in the database.
        "Best" tiers have lower rank values.
        """
        updated_user_tiers = self.computeRanks(last_ts, 5)
        for rank in range(1, len(updated_user_tiers) + 1):
            self.addTier(rank)
            tier_users = updated_user_tiers[rank-1]
            self.tiers[rank] = tier_users

    def saveTiers(self):
        from datetime import datetime
        """
        Saves the current tiers into the tiersys.
        Adds/Replaces array of tiers into tiersys object
        Gets current array of uuids and puts them into tier objects
        {{
            _id : DEFINED BY MONGO,
            created_at: datetime,
            tiers : [{
                rank : [],
                uuids : []
            }, {
                rank : [],
                uuids : []
            }]
        }}
        """
        ts = []
        for rank, users in self.tiers.iteritems():
            ts.append({'rank': rank, 'uuids': users})

        get_tiersys_db().insert_one({'tiers': ts, 'created_at': datetime.now()})

def m_to_km(distance):
    return max(0, distance / 1000)
