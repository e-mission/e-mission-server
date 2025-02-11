import os
import subprocess
import importlib
import logging
import tempfile
import time

import emission.core.get_database as edb

DB_HOST_TEMPLATE = os.environ.get('DB_HOST_TEMPLATE', "mongodb://localhost:27017/openpath_prod_REPLACEME")

if 'PROD_LIST' in os.environ:
    PROD_LIST=os.environ['PROD_LIST'].split(",")
else:
    with tempfile.TemporaryDirectory() as tmpdirname:
        print(f"created {tmpdirname=} to find list of configs")
        os.chdir(tmpdirname)
        proc = subprocess.run(
              f"git clone https://github.com/e-mission/nrel-openpath-deploy-configs.git", shell=True)
        filenames = os.listdir(f"nrel-openpath-deploy-configs/configs/")

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
    print(f'About to run {fn_to_run.__name__} on {len(PROD_LIST)} deployments. Proceed? [y/n]')
    if input() != 'y':
        print("Aborting")
        return
    for prod in PROD_LIST:
        prod_db_name = prod.replace("-", "_")
        print(f"Running {fn_to_run.__name__} for {prod} on DB {prod_db_name}")
        os.environ['DB_HOST'] = DB_HOST_TEMPLATE.replace(
            "REPLACEME", prod_db_name)
        importlib.reload(edb)
        fn_to_run()
