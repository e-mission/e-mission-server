import logging
import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta
import arrow


def find_inactive_users():
    inactive_users = []
    inactive_users_new_consent = ""
    inactive_users_old_consent = ""
    one_week_ago_ts = arrow.utcnow().replace(weeks=-1).timestamp
    for user in edb.get_uuid_db().find():
        db = esta.TimeSeries.get_time_series(user['uuid']).get_data_df("stats/server_api_time", time_query=None)
        new_consent = esta.TimeSeries.get_time_series(user['uuid']).get_data_df("config/consent", time_query=None)
        if db.empty:
            inactive_users.append((user['user_email'], user['update_ts']))                                  
            if new_consent.empty:
                inactive_users_new_consent+=str(user['user_email'])+', '
            else:
                inactive_users_old_consent+=str(user['user_email'])+', '
        else:
            #check last usercache call: 
            #the user is inactive if there are no calls or if the last one was before one_week_ago_ts
            last_usercache_call = db[db['name'].str.contains('usercache', case=False)].tail(1)
            if last_usercache_call.empty:
                inactive_users.append((user['user_email'], user['update_ts']))
                if new_consent.empty:
                    inactive_users_new_consent+=str(user['user_email'])+', '
                else:
                    inactive_users_old_consent+=str(user['user_email'])+', '
            elif last_usercache_call.iloc[0]['ts'] < one_week_ago_ts:
                inactive_users.append((user['user_email'], user['update_ts']))
                if new_consent.empty:
                    inactive_users_new_consent+=str(user['user_email'])+', '
                else:
                    inactive_users_old_consent+=str(user['user_email'])+', '
    print "\nList of inactive users emails and date they signed up:"
    for i in inactive_users:
        print i
    print "\nEmails of inactive users who consented to the new IRB protocol:"
    print inactive_users_new_consent[:-2]
    print "\nEmails of inactive users who did not consent to the new IRB protocol:"
    print inactive_users_old_consent[:-2]
    return


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    find_inactive_users()

