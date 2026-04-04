import os
import arrow
import json
import emission.core.get_database as edb
import emission.storage.json_wrappers as esj
import bin.debug.common as common
from bin.federation import run_on_all_deployments

NOW_SECONDS = arrow.now().timestamp()
user_profiles = []

def find_inactive_uuids(uuids_entries, threshold):
    db = os.environ['DB_HOST'].split('?')[0].split('/')[-1]
    user_profiles = []

    for i, u in enumerate(uuids_entries):
        print(f'Checking activity for user {u["uuid"]} ({i+1}/{len(uuids_entries)})')
        profile_data = edb.get_profile_db().find_one({'user_id': u["uuid"]})
        user_profiles.append(profile_data)

    with open(f'/tmp/profile_dumps/{db}-{NOW_SECONDS}.json', 'w') as fd:
        json.dump(user_profiles, fd, default=esj.wrapped_default)

    return [
        u['user_id'] for u in user_profiles
        if (u.get('last_call_ts') or -1) <= NOW_SECONDS - threshold
        and (u.get('last_loc_ts') or -1) <= NOW_SECONDS - threshold
    ]

def purge_users(uuids):
    print(f'About to remove {len(uuids)} users. Proceed? [y/n]')
    if input() != 'y':
        print("Aborting")
        return
    for u in uuids:
        print(f'Purging user {u}')
        common.purge_entries_for_user(u, True)

def start_inactive(threshold_s, purge):
    uuids_entries = [e for e in edb.get_uuid_db().find()]
    print(f'Total users: {len(uuids_entries)}')
    print('Finding inactive users...')
    inactive_uuids = find_inactive_uuids(uuids_entries, threshold_s)
    print(f'Of {len(uuids_entries)} users, found {len(inactive_uuids)} inactive users:')
    print(inactive_uuids)

    if purge:
        purge_users(inactive_uuids)

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

    run_on_all_deployments(start_inactive, threshold_s, args.purge)


