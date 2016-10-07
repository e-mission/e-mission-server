import json
import logging
import argparse
import numpy as np

import emission.pipeline.scheduler as eps

if __name__ == '__main__':
    try:
        intake_log_config = json.load(open("conf/log/intake.conf", "r"))
    except:
        intake_log_config = json.load(open("conf/log/intake.conf.sample", "r"))

    intake_log_config["handlers"]["file"]["filename"] = intake_log_config["handlers"]["file"]["filename"].replace("intake", "intake_launcher")
    intake_log_config["handlers"]["errors"]["filename"] = intake_log_config["handlers"]["errors"]["filename"].replace("intake", "intake_launcher")

    logging.config.dictConfig(intake_log_config)
    np.random.seed(61297777)

    parser = argparse.ArgumentParser()
    parser.add_argument("n_workers", type=int,
                        help="the number of worker processors to use")
    parser.add_argument("-p", "--public", action="store_true",
        help="pipeline for public (as opposed to regular) phones")
    args = parser.parse_args()

    split_lists = eps.get_split_uuid_lists(args.n_workers, args.public)
    logging.info("Finished generating split lists %s" % split_lists)
    eps.dispatch(split_lists, args.public)
