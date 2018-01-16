class SuggestionSys:
    def __init__(self):
        self.calculate_suggestions()

    def calculate_suggestions(self, last_ts):
        #For each person, create their most recent suggestion
        user_carbon_map = {}
        all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"uuid": 1, "_id": 0})))
        #unfinished, not sure if right direction yet
        for index, row in all_users.iterrows():
            user_id = row['uuid']
            user_carbon_map[user_id] = self.computeCarbon(user_id, TierSys.getLatest()['created_at'])
