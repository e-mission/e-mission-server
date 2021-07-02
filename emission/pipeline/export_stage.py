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
import emission.exportdata.export_data as eeded

def run_export_pipeline(process_number, uuid_list):
    try:
        with open("conf/log/export.conf", "r") as cf:
            export_log_config = json.load(cf)
    except:
        with open("conf/log/export.conf.sample", "r") as cf:
            export_log_config = json.load(cf)

    export_log_config["handlers"]["file"]["filename"] = \
        export_log_config["handlers"]["file"]["filename"].replace("export", "export_%s" % process_number)
    export_log_config["handlers"]["errors"]["filename"] = \
        export_log_config["handlers"]["errors"]["filename"].replace("export", "export_%s" % process_number)

    logging.config.dictConfig(export_log_config)
    np.random.seed(61297777)

    logging.info("processing UUID list = %s" % uuid_list)

    for uuid in uuid_list:
        if uuid is None:
            continue

        # Skip entry with mixed time and distance filters
        if uuid == UUID("2c3996d1-49b1-4dce-82f8-d0cda85d3475"):
            continue

        try:
            run_export_pipeline_for_user(uuid)
        except Exception as e:
            esds.store_pipeline_error(uuid, "WHOLE_PIPELINE", time.time(), None)
            logging.exception("Found error %s while processing pipeline "
                              "for user %s, skipping" % (e, uuid))


def run_export_pipeline_for_user(uuid):
    #run the intake stage?
    with ect.Timer() as edt:
        logging.info("*" * 10 + "UUID %s: exporting data" % uuid + "*" * 10)
        print(str(arrow.now()) + "*" * 10 + "UUID %s: exporting data" % uuid + "*" * 10)
        eeded.export_data(uuid)

    esds.store_pipeline_time(uuid, ecwp.PipelineStages.EXPORT_DATA.name,
                             time.time(), edt.elapsed)
