import arrow
import pymongo
import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta
import bin.debug.common as common
from _common import run_on_all_deployments

SECONDS_90_DAYS = 60 * 60 * 24 * 90

NOW_SECONDS = arrow.now().timestamp()

def find_inactive_uuids(uuids_entries):
    '''
    Users are inactive if:
     - no API calls in the last 90 days
      AND
     - no locations in the last 90 days
    '''
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
        if last_call_ts > NOW_SECONDS - SECONDS_90_DAYS:
            continue

        last_loc_ts = ts.get_first_value_for_field(
            key='background/location',
            field='data.ts',
            sort_order=pymongo.DESCENDING
        )
        print(f'for user {u["uuid"]}, last location was {last_loc_ts}')
        if last_loc_ts > NOW_SECONDS - SECONDS_90_DAYS:
            continue

        print(f'User {u["uuid"]} is inactive')
        inactive_uuids.append(u['uuid'])

    return inactive_uuids


def purge_inactive_users():
    total_users = edb.get_uuid_db().count_documents({})
    print(f'Total users: {total_users}')
    uuids_entries = edb.get_uuid_db().find()
    print('Finding inactive users...')
    inactive_uuids = find_inactive_uuids(uuids_entries)
    print(f'Of {total_users} users, found {len(inactive_uuids)} inactive users:')
    print(inactive_uuids)

    print("Purging inactive users...")
    for u in inactive_uuids:
        print(f'Purging user {u}')
        common.purge_entries_for_user(u, True)


if __name__ == '__main__':
    run_on_all_deployments(purge_inactive_users)
