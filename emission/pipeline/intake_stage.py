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

import emission.core.get_database as edb
import emission.core.timer as ect

import emission.core.wrapper.pipelinestate as ecwp

import emission.net.usercache.abstract_usercache_handler as euah
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.aggregate_timeseries as estag

import emission.analysis.userinput.matcher as eaum
import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.intake.cleaning.clean_and_resample as eaicr
import emission.analysis.classification.inference.mode.pipeline as eacimp
import emission.net.ext_service.habitica.executor as autocheck

import emission.storage.decorations.stats_queries as esds


def run_intake_pipeline(process_number, uuid_list):
    """
    Run the intake pipeline with the specified process number and uuid list.
    Note that the process_number is only really used to customize the log file name
    We could avoid passing it in by using the process id - os.getpid() instead, but
    then we won't get the nice RotatingLogHandler properties such as auto-deleting
    files if there are too many. Maybe it will work properly with logrotate? Need to check
    :param process_number: id representing the process number. In range (0..n)
    :param uuid_list: the list of UUIDs that this process will handle
    :return:
    """
    try:
        with open("conf/log/intake.conf", "r") as cf:
            intake_log_config = json.load(cf)
    except:
        with open("conf/log/intake.conf.sample", "r") as cf:
            intake_log_config = json.load(cf)

    intake_log_config["handlers"]["file"]["filename"] = \
        intake_log_config["handlers"]["file"]["filename"].replace("intake", "intake_%s" % process_number)
    intake_log_config["handlers"]["errors"]["filename"] = \
        intake_log_config["handlers"]["errors"]["filename"].replace("intake", "intake_%s" % process_number)

    logging.config.dictConfig(intake_log_config)
    np.random.seed(61297777)

    logging.info("processing UUID list = %s" % uuid_list)

    for uuid in uuid_list:
        if uuid is None:
            continue

        # Skip entry with mixed time and distance filters
        if uuid == UUID("2c3996d1-49b1-4dce-82f8-d0cda85d3475"):
            continue

        try:
            run_intake_pipeline_for_user(uuid)
        except Exception as e:
            esds.store_pipeline_error(uuid, "WHOLE_PIPELINE", time.time(), None)
            logging.exception("Found error %s while processing pipeline "
                              "for user %s, skipping" % (e, uuid))

def run_intake_pipeline_for_user(uuid):
        uh = euah.UserCacheHandler.getUserCacheHandler(uuid)

        with ect.Timer() as uct:
            logging.info("*" * 10 + "UUID %s: moving to long term" % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: moving to long term" % uuid + "*" * 10)
            uh.moveToLongTerm()

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.USERCACHE.name,
                                 time.time(), uct.elapsed)

        with ect.Timer() as uit:
            logging.info("*" * 10 + "UUID %s: updating incoming user inputs" % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: updating incoming user inputs" % uuid + "*" * 10)
            eaum.match_incoming_user_inputs(uuid)

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.USER_INPUT_MATCH_INCOMING.name,
                                 time.time(), uct.elapsed)

        # Hack until we delete these spurious entries
        # https://github.com/e-mission/e-mission-server/issues/407#issuecomment-2484868
        # Hack no longer works after the stats are in the timeseries because
        # every user, even really old ones, have the pipeline run for them,
        # which inserts pipeline_time stats.
        # Let's strip out users who only have pipeline_time entries in the timeseries
        # I wonder if this (distinct versus count) is the reason that the pipeline has
        # become so much slower recently. Let's try to actually delete the
        # spurious entries or at least mark them as obsolete and see if that helps.
        if edb.get_timeseries_db().find({"user_id": uuid}).distinct("metadata.key") == ["stats/pipeline_time"]:
            logging.debug("Found no entries for %s, skipping" % uuid)
            return

        with ect.Timer() as aft:
            logging.info("*" * 10 + "UUID %s: filter accuracy if needed" % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: filter accuracy if needed" % uuid + "*" * 10)
            eaicf.filter_accuracy(uuid)

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.ACCURACY_FILTERING.name,
                                 time.time(), aft.elapsed)

        with ect.Timer() as tst:
            logging.info("*" * 10 + "UUID %s: segmenting into trips" % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: segmenting into trips" % uuid + "*" * 10)
            eaist.segment_current_trips(uuid)

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.TRIP_SEGMENTATION.name,
                                 time.time(), tst.elapsed)

        with ect.Timer() as sst:
            logging.info("*" * 10 + "UUID %s: segmenting into sections" % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: segmenting into sections" % uuid + "*" * 10)
            eaiss.segment_current_sections(uuid)

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.SECTION_SEGMENTATION.name,
                                 time.time(), sst.elapsed)

        with ect.Timer() as jst:
            logging.info("*" * 10 + "UUID %s: smoothing sections" % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: smoothing sections" % uuid + "*" * 10)
            eaicl.filter_current_sections(uuid)

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.JUMP_SMOOTHING.name,
                                 time.time(), jst.elapsed)

        with ect.Timer() as crt:
            logging.info("*" * 10 + "UUID %s: cleaning and resampling timeline" % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: cleaning and resampling timeline" % uuid + "*" * 10)
            eaicr.clean_and_resample(uuid)

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.CLEAN_RESAMPLING.name,
                                 time.time(), crt.elapsed)

        with ect.Timer() as crt:
            logging.info("*" * 10 + "UUID %s: inferring transportation mode" % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: inferring transportation mode" % uuid + "*" * 10)
            eacimp.predict_mode(uuid)

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.MODE_INFERENCE.name,
                                 time.time(), crt.elapsed)

        with ect.Timer() as crt:
            logging.info("*" * 10 + "UUID %s: creating confirmed objects " % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: creating confirmed objects " % uuid + "*" * 10)
            eaum.create_confirmed_objects(uuid)

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.CREATE_CONFIRMED_OBJECTS.name,
                                 time.time(), crt.elapsed)

        with ect.Timer() as ogt:
            logging.info("*" * 10 + "UUID %s: storing views to cache" % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: storing views to cache" % uuid + "*" * 10)
            uh.storeViewsToCache()

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.OUTPUT_GEN.name,
                                 time.time(), ogt.elapsed)
