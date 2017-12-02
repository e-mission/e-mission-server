from __future__ import print_function
import logging
import argparse
import uuid
import emission.core.wrapper.user as ecwu
import emission.core.get_database as edb
import emission.net.ext_service.habitica.proxy as proxy

def fix_autocheck_for_user(uuid):
    auto_tasks = find_existing_auto_tasks(uuid)
    delete_tasks(uuid, auto_tasks)
    create_new_tasks(uuid)

# I wanted to reuse existing code, but it is unclear how to do so.
# in particular, I will have either the format of the old tests or of
# the new tests. Most PRs will not keep the old and the new around side
# to side. Since this is a historical, as opposed to ongoing script, I
# think this is fine.

def find_existing_auto_tasks(uuid):
    method_uri = "/api/v3/tasks/user"
    get_habits_uri = method_uri + "?type=habits"
    #First, get all habits and check if the habit requested already exists
    result = proxy.habiticaProxy(uuid, 'GET', get_habits_uri, None)
    habits = result.json()
    auto_tasks = []
    for habit in habits['data']:
        print(habit['text'], habit["notes"], habit["id"])
        if "automatically" in habit['notes']:
            logging.debug("Found auto task %s, %s, %s" %
                          (habit['text'], habit['notes'], habit['id']))
            auto_tasks.append(habit)
        else:
            if len(habit["challenge"]) > 0:
                logging.info("Found challenge task %s, %s, %s, unsure what to do" %
                             (habit['text'], habit['notes'], habit['id']))
            else:
                logging.debug("Found manual task %s, %s, %s" %
                              (habit['text'], habit['notes'], habit['id']))
    return auto_tasks

def delete_tasks(uuid, task_list):
    method_uri = "/api/v3/tasks/"

    for task in task_list:
        curr_task_del_uri = method_uri + str(task["id"])
        result = proxy.habiticaProxy(uuid, 'DELETE', curr_task_del_uri, {})
        logging.debug("Result of deleting %s = %s" % (task["id"], result.json()))

def create_new_tasks(uuid):
    bike_walk_habit = {'type': "habit", 'text': "Bike and Walk", 'notes': "Automatically get points for every 1 km walked or biked. ***=== DO NOT EDIT BELOW THIS POINT ===*** AUTOCHECK: {\"mapper\": \"active_distance\", \"args\": {\"walk_scale\": 1000, \"bike_scale\": 1000}}", 'up': True, 'down': False, 'priority': 2}
    bike_walk_habit_id = proxy.create_habit(uuid, bike_walk_habit)

    invite_friends = {'type': "habit", 'text': "Spread the word", 'notes': "Get points for inviting your friends! We're better together.", 'up': True, 'down': False, 'priority': 2}
    invite_friends_id = proxy.create_habit(uuid, invite_friends)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-e", "--user_email")
    group.add_argument("-u", "--user_uuid")
    group.add_argument("-a", "--all", action="store_true")

    args = parser.parse_args()

    if args.all:
        for uuid in edb.get_habitica_db().distinct("user_id"):
            logging.debug("About to check user %s" % uuid)
            fix_autocheck_for_user(uuid)
    else:
        if args.user_uuid:
            del_uuid = uuid.UUID(args.user_uuid)
        else:
            del_uuid = ecwu.User.fromEmail(args.user_email).uuid

        fix_autocheck_for_user(del_uuid)
