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
import emission.purge_restore.purge_data as eprpd

def run_purge_pipeline(process_number, uuid, archive_dir=None):
    try:
        with open("conf/log/purge.conf", "r") as cf:
            purge_log_config = json.load(cf)
    except:
        with open("conf/log/purge.conf.sample", "r") as cf:
            purge_log_config = json.load(cf)

    purge_log_config["handlers"]["file"]["filename"] = \
        purge_log_config["handlers"]["file"]["filename"].replace("purge", "purge_%s" % process_number)
    purge_log_config["handlers"]["errors"]["filename"] = \
        purge_log_config["handlers"]["errors"]["filename"].replace("purge", "purge_%s" % process_number)

    # logging.config.dictConfig(purge_log_config)
    np.random.seed(61297777)

    logging.info("processing UUID list = %s" % uuid)

    try:
        file_name = run_purge_pipeline_for_user(uuid, archive_dir)
    except Exception as e:
        esds.store_pipeline_error(uuid, "WHOLE_PIPELINE", time.time(), None)
        logging.exception("Found error %s while processing pipeline "
                            "for user %s, skipping" % (e, uuid))
            
    return file_name


def run_purge_pipeline_for_user(uuid, archive_dir):
    with ect.Timer() as edt:
        logging.info("*" * 10 + "UUID %s: purging timeseries data" % uuid + "*" * 10)
        print(str(arrow.now()) + "*" * 10 + "UUID %s: purging timeseries data" % uuid + "*" * 10)
        file_name = eprpd.purge_data(uuid, archive_dir)

    esds.store_pipeline_time(uuid, ecwp.PipelineStages.PURGE_TIMESERIES_DATA.name,
                             time.time(), edt.elapsed)

    return file_name
