from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import *
import json
import logging
import numpy as np
import arrow
from uuid import UUID
import time

import emission.core.timer as ect

import emission.core.wrapper.pipelinestate as ecwp

import emission.storage.decorations.stats_queries as esds
import emission.purge_restore.restore_data as eprrd

def run_restore_pipeline(process_number, uuid_list, file_names):
    try:
        with open("conf/log/restore.conf", "r") as cf:
            restore_log_config = json.load(cf)
    except:
        with open("conf/log/restore.conf.sample", "r") as cf:
            restore_log_config = json.load(cf)

    restore_log_config["handlers"]["file"]["filename"] = \
        restore_log_config["handlers"]["file"]["filename"].replace("restore", "restore_%s" % process_number)
    restore_log_config["handlers"]["errors"]["filename"] = \
        restore_log_config["handlers"]["errors"]["filename"].replace("restore", "restore_%s" % process_number)

    # logging.config.dictConfig(restore_log_config)
    np.random.seed(61297777)

    logging.info("processing UUID list = %s" % uuid_list)

    for uuid in uuid_list:
        if uuid is None:
            continue

        try:
            run_restore_pipeline_for_user(uuid, file_names)
        except Exception as e:
            esds.store_pipeline_error(uuid, "WHOLE_PIPELINE", time.time(), None)
            logging.exception("Found error %s while processing pipeline "
                              "for user %s, skipping" % (e, uuid))


def run_restore_pipeline_for_user(uuid, file_names):
    with ect.Timer() as edt:
        logging.info("*" * 10 + "UUID %s: restoring timeseries data" % uuid + "*" * 10)
        print(str(arrow.now()) + "*" * 10 + "UUID %s: restoring timeseries data" % uuid + "*" * 10)
        eprrd.restore_data(uuid, file_names)

    esds.store_pipeline_time(uuid, ecwp.PipelineStages.RESTORE_TIMESERIES_DATA.name,
                             time.time(), edt.elapsed)
