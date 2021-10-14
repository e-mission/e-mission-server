import arrow
import logging
import argparse
import emission.core.wrapper.pipelinestate as ecwp
import emission.core.get_database as edb

# Run in containers using:
# sudo docker exec $CONTAINER bash -c 'cd e-mission-server; source setup/activate.sh; ./e-mission-py.bash bin/debug/find_invalid_pipeline_state.py'

def print_all_invalid_state():
    all_invalid_states = edb.get_pipeline_state_db().find({"curr_run_ts": {"$ne": None}})
    for invalid_state in all_invalid_states:
        print(f"{invalid_state.user_id}: {ecwp.PipelineStage(invalid_state.pipeline_stage)} set to {arrow.get(invalid_state.curr_run_ts)}")

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(prog="find_invalid_pipeline_state")
    args = parser.parse_args()
    print_all_invalid_state()
