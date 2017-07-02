import logging
import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta
import arrow
import pandas as pd


def find_inactive_users():
    inactive_users = []
    inactive_users_new_consent = ""
    inactive_users_old_consent = ""
    inactive_users_before_september = ""
    inactive_users_after_september = ""
    one_week_ago_ts = arrow.utcnow().replace(weeks=-1).timestamp
    september_first = arrow.get('2016-09-01').timestamp
    for user in edb.get_uuid_db().find():
        db = esta.TimeSeries.get_time_series(user['uuid']).get_data_df("stats/server_api_time", time_query=None)
        new_consent = esta.TimeSeries.get_time_series(user['uuid']).get_data_df("config/consent", time_query=None)
        signup_date = arrow.get(user['update_ts'])
        if db.empty:
            inactive_users.append((user['user_email'], signup_date.date(), ()))
            if new_consent.empty:
                inactive_users_new_consent+=str(user['user_email'])+', '
            else:
                inactive_users_old_consent+=str(user['user_email'])+', '
            if signup_date.timestamp < september_first:
                inactive_users_before_september+=str(user['user_email'])+', '
            else:
                inactive_users_after_september+=str(user['user_email'])+', '
        else:
            #check last usercache call: 
            #the user is inactive if there are no calls or if the last one was before one_week_ago_ts
            last_usercache_call = db[db['name'].str.contains('usercache', case=False)].tail(1)
            if last_usercache_call.empty:
                inactive_users.append((user['user_email'], signup_date.date(), ()))
                if new_consent.empty:
                    inactive_users_new_consent+=str(user['user_email'])+', '
                else:
                    inactive_users_old_consent+=str(user['user_email'])+', '
                if signup_date.timestamp < september_first:
                    inactive_users_before_september+=str(user['user_email'])+', '
                else:
                    inactive_users_after_september+=str(user['user_email'])+', '
            else:
                if last_usercache_call.iloc[0]['ts'] < one_week_ago_ts:
                    inactive_users.append((user['user_email'], signup_date.date(), arrow.get(last_usercache_call.iloc[0]['ts']).date()))
                    if new_consent.empty:
                        inactive_users_new_consent+=str(user['user_email'])+', '
                    else:
                        inactive_users_old_consent+=str(user['user_email'])+', '
                    if signup_date.timestamp < september_first:
                        inactive_users_before_september+=str(user['user_email'])+', '
                    else:
                        inactive_users_after_september+=str(user['user_email'])+', '
    inactive_users_table = pd.DataFrame(inactive_users, columns=['Email', 'Last Sign Up Date', 'Last Usercache Call'])
    print "\nList of inactive users emails and date they signed up:"
    print inactive_users_table
    print "\nEmails of inactive users who consented to the new IRB protocol:"
    print inactive_users_new_consent[:-2]
    print "\nEmails of inactive users who did not consent to the new IRB protocol:"
    print inactive_users_old_consent[:-2]
    print "\nEmails of inactive users who signed up before September 1st:"
    print inactive_users_before_september[:-2]
    print "\nEmails of inactive users who signed up after September 1st:"
    print inactive_users_after_september[:-2]   
    return


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    find_inactive_users()

