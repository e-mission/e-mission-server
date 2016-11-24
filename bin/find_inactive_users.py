import logging
import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta
import arrow


def find_inactive_users():
    inactive_users = ""
    one_week_ago_ts = arrow.utcnow().replace(weeks=-1).timestamp
    for user in edb.get_uuid_db().find():
        db = esta.TimeSeries.get_time_series(user['uuid']).get_data_df("stats/server_api_time", time_query=None)
        if db.empty:
            inactive_users+=str(user['user_email'])+', '
        else:
            #check last usercache call: 
            #the user is inactive if there are no calls or if the last one was before one_week_ago_ts
            last_usercache_call = db[db['name'].str.contains('usercache', case=False)].tail(1)
            if last_usercache_call.empty:
                inactive_users+=str(user['user_email'])+', '
            elif last_usercache_call.iloc[0]['ts'] < one_week_ago_ts:
                inactive_users+=str(user['user_email'])+', '
    return inactive_users[:-2]

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    print find_inactive_users()

