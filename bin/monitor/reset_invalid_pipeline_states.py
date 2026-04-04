import arrow
import logging
import argparse
import pandas as pd

from bin.federation import run_on_all_deployments
import emission.core.wrapper.pipelinestate as ecwp
import emission.core.get_database as edb
import emission.pipeline.reset as epr

# Run in containers using:
# sudo docker exec $CONTAINER bash -c 'cd e-mission-server; source setup/activate.sh; ./e-mission-py.bash bin/debug/reset_invalid_pipeline_state.py'

def reset_all_invalid_state(args):
    epr.auto_reset(args.dry_run, args.only_calc)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(prog="reset_invalid_pipeline_stage")
    parser.add_argument("-c", "--only_calc", action="store_true", default=False,
                        help="only calculate the reset timestamps, don't launch the reset process")
    parser.add_argument("-n", "--dry_run", action="store_true", default=False,
                        help="do everything except actually perform the operations")

    parser.add_argument("-f", "--federated", action="store_true", default=False,
                        help="Run against all active deployments")

    args = parser.parse_args()
    if not args.federated:
        reset_all_invalid_state(args)
    else:
        run_on_all_deployments(reset_all_invalid_state, args)
