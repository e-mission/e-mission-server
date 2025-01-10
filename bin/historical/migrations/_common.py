import os
import subprocess
import importlib
import logging

import emission.core.get_database as edb

DB_HOST_TEMPLATE = "mongodb://localhost:27017/openpath_prod_REPLACEME"

proc = subprocess.run(
      'rm -rf nrel-openpath-deploy-configs && ' +
      'git clone --no-checkout https://github.com/e-mission/nrel-openpath-deploy-configs.git && ' +
      'cd nrel-openpath-deploy-configs && ' +
      'git ls-tree -r --name-only HEAD | grep configs/',
      shell=True,
      capture_output=True,
      text=True)
filenames = proc.stdout.replace("configs/", "").split("\n")

PROD_LIST = [
    fname.split(".")[0]
    for fname in filenames
      if fname and 'dev-' not in fname and 'stage-' not in fname
]
print(f"PROD_LIST: {PROD_LIST}")


def run_on_all_deployments(fn_to_run):
    """
    Run the given function on the database for each deployment by setting the
      DB_HOST environment variable in between each function call.
    The list of deployments (PROD_LIST) is retrieved from the
      nrel-openpath-deploy-configs repo upon initialization of this module.
    """
    for prod in PROD_LIST:
        prod_db_name = prod.replace("-", "_")
        print(f"Running {fn_to_run.__name__} for {prod} on DB {prod_db_name}")
        os.environ['DB_HOST'] = DB_HOST_TEMPLATE.replace(
            "REPLACEME", prod_db_name)
        importlib.reload(edb)
        fn_to_run()
