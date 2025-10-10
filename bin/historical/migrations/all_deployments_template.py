import emission.core.get_database as edb

from _common import run_on_all_deployments

def print_connect_url():
    print(f"Connecting to database {edb.url}, with collections {edb._current_db.list_collection_names()}")

run_on_all_deployments(print_connect_url)
