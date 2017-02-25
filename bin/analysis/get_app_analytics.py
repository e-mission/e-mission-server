import logging
import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
import matplotlib.dates as mdates


def get_app_analytics():
    df = pd.DataFrame()
    for user in edb.get_uuid_db().find():
        user_df = esta.TimeSeries.get_time_series(user['uuid']).get_data_df("stats/server_api_time", time_query=None)
        if not user_df.empty:
            df = df.append(user_df, ignore_index = True)

    df['datetime'] = df.ts.apply(lambda ts: dt.datetime.fromtimestamp(ts))
    df.ix[df.reading>1, 'reading'] = 1
    fig, ax = plt.subplots()
    ax.xaxis.set_major_locator(mdates.WeekdayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d/%Y"))
    plt.ylabel('Response time')

    dashboard_df = df[df.name == "POST_/result/metrics/timestamp"]
    dashboard_df.plot(x="datetime", y="reading", ax=ax, style='+', legend=None)
    plt.title('Dashboard')
    fig.savefig('Dashboard.png')
    plt.close(fig)

    fig, ax = plt.subplots()
    cache_put_df = df[df.name == "POST_/usercache/put"]
    cache_put_df.plot(x="datetime", y="reading", ax=ax, style='+', legend=None)
    plt.title('Usercache_put')
    fig.savefig('Usercache_put.png')
    plt.close(fig)

    fig, ax = plt.subplots()
    cache_get_df = df[df.name == "POST_/usercache/get"]
    cache_get_df.plot(x="datetime", y="reading", ax=ax, style='+', legend=None)
    plt.title('Usercache_get')
    fig.savefig('Usercache_get.png')
    plt.close(fig)

    fig, ax = plt.subplots()
    stats_set_df = df[df.name == "POST_/stats/set"]
    stats_set_df.plot(x="datetime", y="reading", ax=ax, style='+', legend=None)
    plt.title('Stats_set')
    fig.savefig('Stats_set.png')
    plt.close(fig)

    fig, ax = plt.subplots()
    habitica_intro_df = df[df.name == "POST_/habiticaRegister"]
    habitica_intro_df.plot(x="datetime", y="reading", ax=ax, style='+', legend=None)
    plt.title('Habitica Sign up and Login')
    fig.savefig('Habitica Sign up_Login.png')
    plt.close(fig)

    fig, ax = plt.subplots()
    habitica_df = df[df.name == "POST_/habiticaProxy"]
    habitica_df.plot(x="datetime", y="reading", ax=ax, style='+', legend=None)
    plt.title('Habitica')
    fig.savefig('Habitica.png')
    plt.close(fig)

    fig, ax = plt.subplots()
    diary_df = df[df.name.str.contains("POST_/timeline/getTrips")]
    diary_df.plot(x="datetime", y="reading", ax=ax, style='+', legend=None)
    plt.title('Diary')
    fig.savefig('Diary.png')
    plt.close(fig)
    return


def get_aggregate_analytics():
    df = pd.DataFrame()
    for user in edb.get_uuid_db().find():
        user_df = esta.TimeSeries.get_time_series(user['uuid']).get_data_df("stats/server_api_time", time_query=None)
        if not user_df.empty:
            df = df.append(user_df, ignore_index = True)
            
    df['datetime'] = df.ts.apply(lambda ts: dt.datetime.fromtimestamp(ts))
    df.ix[df.reading>1, 'reading'] = 1
    fig, ax = plt.subplots()
    ax.xaxis.set_major_locator(mdates.WeekdayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d/%Y"))
    plt.ylabel('Response time')
    plt.title('App Analytics')

    f_df = df[df.name == "POST_/result/metrics/timestamp"]
    f_df.plot(x="datetime", y="reading", ax=ax, style='+', color='g', label='Dashboard')

    f_df = df[df.name == "POST_/usercache/put"]
    f_df.plot(x="datetime", y="reading", ax=ax, style='+', color='b', label='Usercache_put')

    f_df = df[df.name == "POST_/usercache/get"]
    f_df.plot(x="datetime", y="reading", ax=ax, style='+', color='r', label='Usercache_get')

    f_df = df[df.name == "POST_/stats/set"]
    f_df.plot(x="datetime", y="reading", ax=ax, style='+', color='black', label='Stats_set')

    f_df = df[df.name == "POST_/habiticaRegister"]
    f_df.plot(x="datetime", y="reading", ax=ax, style='+', color='orange', label='Habitica Sign up_Login')

    f_df = df[df.name == "POST_/habiticaProxy"]
    f_df.plot(x="datetime", y="reading", ax=ax, style='+', color='aqua', label='Habitica')

    f_df = df[df.name.str.contains("POST_/timeline/getTrips")]
    f_df.plot(x="datetime", y="reading", ax=ax, style='+', color='m', label='Diary')

    plt.legend()
     
    fig.savefig('app_analytics.png')
    fig.savefig('app_analytics.eps', format='eps', dpi=1000)
    return

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    get_app_analytics()
    get_aggregate_analytics()
