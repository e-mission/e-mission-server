# Initial attempt at a task definition. This is the data structure -
# the parsing and interpreting code is elsewhere
import logging
import attrdict as ad
import json

import emission.net.ext_service.habitica.proxy as hp
import emission.net.ext_service.habitica.auto_tasks.task as enehat

def give_points_for_all_tasks(user_id):
    # Get the tasks from habitica
    logging.debug("Entering habitica autocheck for user %s" % user_id)
    habitica_task_result = get_tasks_from_habitica(user_id)
    logging.debug("Retrieved %d from habitica for user %s" % (len(habitica_task_result), user_id))
    give_points_for_tasks(user_id, habitica_task_result["data"])

def give_points_for_tasks(user_id, habitica_tasks):
    """
    Split this out into a separate function to make it easier to test
    We can retrieve habitica tasks, munge them and then pass them through to this
    :param user_id: user id
    :param habitica_tasks: list of habitica tasks
    :return:
    """
    # Filter out manual and convert auto to wrapper
    auto_tasks = get_autocheckable(habitica_tasks)
    logging.debug("after autocheckable filter %s -> %s" % (len(habitica_tasks),
                                                           len(auto_tasks)))
    for task in auto_tasks:
        logging.debug("About to give points for user %s, task %s" % (user_id,
                                                                     task.task_id))
        try:
            give_points_for_task(user_id, task)
        except Exception as e:
            logging.error("While processing task %s, found error %s" %
                          (task.task_id, e))

def give_points_for_task(user_id, task):
    curr_state = get_task_state(user_id, task)
    logging.debug("for task %s, curr_state = %s" % (task.task_id, user_id))
    map_fn = get_map_fn(task.mapper)
    # TODO: Figure out if we should pass in the args separately
    new_state = map_fn(user_id, task, curr_state)
    logging.debug("after running mapping function %s for task %s, new_state = %s" %
                  (map_fn, task.task_id, new_state))
    save_task_state(user_id, task, new_state)

def reset_all_tasks_to_ts(user_id, ts, is_dry_run):
    # Get the tasks from habitica
    logging.debug("Entering habitica autocheck for user %s" % user_id)
    habitica_task_result = get_tasks_from_habitica(user_id)
    logging.debug("Retrieved %d from habitica for user %s" % (len(habitica_task_result), user_id))
    reset_tasks_to_ts(user_id, ts, habitica_task_result["data"], is_dry_run)

def reset_tasks_to_ts(user_id, ts, habitica_tasks, is_dry_run):
    """
    Split this out into a separate function to make it easier to test
    We can retrieve habitica tasks, munge them and then pass them through to this
    :param user_id: user id
    :param habitica_tasks: list of habitica tasks
    :return:
    """
    # Filter out manual and convert auto to wrapper
    auto_tasks = get_autocheckable(habitica_tasks)
    logging.debug("after autocheckable filter %s -> %s" % (len(habitica_tasks),
                                                           len(auto_tasks)))
    for task in auto_tasks:
        logging.debug("About to give points for user %s, task %s" % (user_id,
                                                                     task.task_id))
        try:
            reset_task_to_ts(user_id, ts, is_dry_run, task)
        except Exception as e:
            logging.error("While processing task %s, found error %s" %
                          (task.task_id, e))

def reset_task_to_ts(user_id, ts, is_dry_run, task):
    curr_state = get_task_state(user_id, task)
    logging.debug("for task %s, curr_state = %s" % (task.task_id, user_id))
    reset_fn = get_reset_fn(task.mapper)
    # TODO: Figure out if we should pass in the args separately
    new_state = reset_fn(user_id, ts, task, curr_state)
    logging.debug("after running mapping function %s for task %s, new_state = %s" %
                  (reset_fn, task.task_id, new_state))
    if is_dry_run:
        logging.info("is_dry_run = True, not saving the state")
    else:
        save_task_state(user_id, task, new_state)

def get_tasks_from_habitica(user_id):
    tasks_uri = "/api/v3/tasks/user"
    # Get all tasks from the user
    try:
        result = hp.habiticaProxy(user_id, 'GET', tasks_uri, None)
        tasks = result.json()
    except AssertionError as e:
        logging.info("User %s has not registered for habitica, returning empty tasks" % user_id)
        tasks = {"data": []}

    logging.debug("For user %s, retrieved %s tasks from habitica" %
                  (user_id, len(tasks)))
    return tasks

def get_autocheckable(tasks):
    ats = []
    for task in tasks:
        try:
            at = to_autocheck_wrapper(task)
            if at is not None:
                logging.debug("wrapped task = %s" % at)
                ats.append(at)
        except Exception as e:
            logging.warning("While converting %s, skipping because of error %s" %
                            (task, e))

    logging.debug("After filtering autocheckable, %s -> %s" %
                  (len(tasks), len(ats)))
    return ats

def to_autocheck_wrapper(task_doc):
    # We're going to put the task information into the notes, after the word AUTOCHECK.
    task_doc_ad = ad.AttrDict(task_doc)
    note = task_doc_ad.notes
    logging.debug("Considering note = %s" % note)
    if "AUTOCHECK:" in note:
        autocheck_formula_start = note.find(":")
        autocheck_formula_str = note[autocheck_formula_start+1:]
        logging.debug("Found autocheck formula %s" % autocheck_formula_str)
        autocheck_formula = json.loads(autocheck_formula_str)
        t = enehat.Task(autocheck_formula)
        t.habitica_task = task_doc
        t.task_id = task_doc_ad.id
        return t
    else:
        logging.debug("no autocheck found")
        return None

# Function to map the name to code
def get_map_fn(fn_name):
    import importlib

    module_name = get_module_name(fn_name)
    logging.debug("module_name = %s" % module_name)
    module = importlib.import_module(module_name)
    return getattr(module, "give_points")

# Function to map the name to code
def get_reset_fn(fn_name):
    import importlib

    module_name = get_module_name(fn_name)
    logging.debug("module_name = %s" % module_name)
    module = importlib.import_module(module_name)
    return getattr(module, "reset_to_ts")

def get_module_name(fn_name):
    return "emission.net.ext_service.habitica.auto_tasks.{key}".format(
        key=fn_name)

# Functions to get and set task state
def get_task_state(user_id, task):
    # TODO: Figure out whether we want to store one large document
    # for all tasks, or multiple small documents, indexed by uuid and task_id
    # For now, one large document since that is what @juemura has done so far
    user_entry = hp.get_user_entry(user_id)
    if "task_state" not in user_entry:
        return None
    task_states = user_entry["task_state"]
    logging.debug("Found states for tasks %s" % task_states)
    if task.task_id in task_states:
        return task_states[task.task_id]
    else:
        return None

def save_task_state(user_id, task, new_state):
    user_entry = hp.get_user_entry(user_id)
    if "task_state" not in user_entry:
        user_entry["task_state"] = {}
    task_states = user_entry["task_state"]
    task_states[task.task_id] = new_state
    hp.save_user_entry(user_id, user_entry)

