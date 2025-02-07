import arrow
import pymongo
import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta
import bin.debug.common as common

NOW_SECONDS = arrow.now().timestamp()

def find_inactive_uuids(uuids_entries, threshold):
    inactive_uuids = []
    for u in uuids_entries:
        print(f'Checking activity for user {u["uuid"]}')
        ts = esta.TimeSeries.get_time_series(u['uuid'])

        last_call_ts = ts.get_first_value_for_field(
            key='stats/server_api_time',
            field='data.ts',
            sort_order=pymongo.DESCENDING
        )
        print(f'for user {u["uuid"]}, last call was {last_call_ts}')
        if last_call_ts > NOW_SECONDS - threshold:
            continue

        last_loc_ts = ts.get_first_value_for_field(
            key='background/location',
            field='data.ts',
            sort_order=pymongo.DESCENDING
        )
        print(f'for user {u["uuid"]}, last location was {last_loc_ts}')
        if last_loc_ts > NOW_SECONDS - threshold:
            continue

        print(f'User {u["uuid"]} is inactive')
        inactive_uuids.append(u['uuid'])

    return inactive_uuids

def purge_users(uuids):
    print(f'About to remove {len(uuids)} users. Proceed? [y/n]')
    if input() != 'y':
        print("Aborting")
        return
    for u in uuids:
        print(f'Purging user {u}')
        common.purge_entries_for_user(u, True)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        prog='inactive_users',
        description='Identify and perform actions on inactive users'
    )
    parser.add_argument('-t', '--threshold', help='amount of time in days that defines an inactive user') 
    parser.add_argument('-p', '--purge', action='store_true', help='purge inactive users')
    args = parser.parse_args()

    threshold_s = 60 * 60 * 24 * int(args.threshold)

    total_users = edb.get_uuid_db().count_documents({})
    print(f'Total users: {total_users}')
    uuids_entries = edb.get_uuid_db().find()
    print('Finding inactive users...')
    inactive_uuids = find_inactive_uuids(uuids_entries, threshold_s)
    print(f'Of {total_users} users, found {len(inactive_uuids)} inactive users:')
    print(inactive_uuids)

    if args.purge:
        purge_users(inactive_uuids)

