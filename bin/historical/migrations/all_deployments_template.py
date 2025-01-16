import emission.core.get_database as edb

from _common import run_on_all_deployments

def print_connect_url():
    print("Connecting to database URL"+edb.url)

run_on_all_deployments(print_connect_url)
