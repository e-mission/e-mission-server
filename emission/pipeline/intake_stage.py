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
import pymongo
from datetime import datetime

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
import emission.analysis.classification.inference.mode.rule_engine as eacimr
import emission.analysis.classification.inference.labels.pipeline as eacilp
import emission.analysis.userinput.expectations as eaue
import emission.analysis.plotting.composite_trip_creation as eapcc
import emission.net.ext_service.habitica.executor as autocheck

import emission.storage.decorations.stats_queries as esds

import emission.core.wrapper.user as ecwu
import emission.analysis.result.user_stat as eaurs

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

        try:
            run_intake_pipeline_for_user(uuid)
        except Exception as e:
            esds.store_pipeline_error(uuid, "WHOLE_PIPELINE", time.time(), None)
            logging.exception("Found error %s while processing pipeline "
                              "for user %s, skipping" % (e, uuid))

def run_intake_pipeline_for_user(uuid):
        user_profile = edb.get_profile_db().find_one({"user_id": uuid})
        # Before this, we used to use a check of "have I moved new entries to long-term"
        # But that fails if a pipeline has been stuck for a long time, and the user is dormant
        # Since we added profile-level metrics to display in the admin
        # dashboard, we can now use those for a more principled check. But in
        # trip segmentation, we elide the last few location points since there
        # is a delay of 5-10 minutes while detecting trip end.
        # - if the user is dormant, we will miss any trips in the last 10 mins, which is NBD
        # - if the user is not dormant, we will handle the locations as part of the next run
        last_loc_ts = arrow.get(user_profile.get('last_location_ts', 0))
        last_proc_time = user_profile.get('pipeline_range', {}).get('end_ts', None)
        last_proc_ts = arrow.get(last_proc_time) if last_proc_time is not None else arrow.get(0)
        ts_diff = last_loc_ts.timestamp() - last_proc_ts.timestamp()
        fmt_dormant_user_check = f"For {uuid=}, last location entry is at {last_loc_ts}({last_loc_ts.timestamp()}), pipeline has run until {last_proc_ts}({last_proc_ts.timestamp()}), difference = {last_loc_ts - last_proc_ts}({(ts_diff)})"
        if  (ts_diff) <= 6 * 60 * 60: # 10 minutes
            print(f"{fmt_dormant_user_check}, skipping")
            return
        else:
            logging.info(f"{fmt_dormant_user_check}, continuing")

        # the pipeline states are a list, so we can directly query for
        in_progress_stages = edb.get_pipeline_state_db().count_documents({"user_id": uuid, "curr_run_ts": {"$ne": None}})
        fmt_in_progress_check = f"Pipeline for {uuid} has {in_progress_stages=}"
        if in_progress_stages > 0:
            print(f"{fmt_in_progress_check}, skipping")
            logging.debug(f"The states are {list(edb.get_pipeline_state_db().find({'user_id': uuid, 'curr_run_ts': {'$ne': None}}))}")
            return
        else:
            logging.info(f"{fmt_in_progress_check}, continuing")

        uh = euah.UserCacheHandler.getUserCacheHandler(uuid)

        with ect.Timer() as uct:
            logging.info("*" * 10 + "UUID %s: moving to long term" % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: moving to long term" % uuid + "*" * 10)
            new_entry_count = uh.moveToLongTerm()

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.USERCACHE.name,
                                 time.time(), uct.elapsed)

        with ect.Timer() as uit:
            logging.info("*" * 10 + "UUID %s: updating incoming user inputs" % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: updating incoming user inputs" % uuid + "*" * 10)
            eaum.match_incoming_user_inputs(uuid)

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.USER_INPUT_MATCH_INCOMING.name,
                                 time.time(), uit.elapsed)

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
            eacimr.predict_mode(uuid)

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.MODE_INFERENCE.name,
                                 time.time(), crt.elapsed)

        with ect.Timer() as crt:
            logging.info("*" * 10 + "UUID %s: inferring labels" % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: inferring labels" % uuid + "*" * 10)
            eacilp.infer_labels(uuid)

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.LABEL_INFERENCE.name,
                                 time.time(), crt.elapsed)

        with ect.Timer() as crt:
            logging.info("*" * 10 + "UUID %s: populating expectations" % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: populating expectations" % uuid + "*" * 10)
            eaue.populate_expectations(uuid)

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.EXPECTATION_POPULATION.name,
                                 time.time(), crt.elapsed)

        with ect.Timer() as crt:
            logging.info("*" * 10 + "UUID %s: creating confirmed objects " % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: creating confirmed objects " % uuid + "*" * 10)
            eaum.create_confirmed_objects(uuid)

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.CREATE_CONFIRMED_OBJECTS.name,
                                 time.time(), crt.elapsed)

        with ect.Timer() as crt:
            logging.info("*" * 10 + "UUID %s: creating composite objects " % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: creating composite objects " % uuid + "*" * 10)
            eapcc.create_composite_objects(uuid)

        esds.store_pipeline_time(uuid, ecwp.PipelineStages.CREATE_COMPOSITE_OBJECTS.name,
                                 time.time(), crt.elapsed)

        with ect.Timer() as gsr:
            logging.info("*" * 10 + "UUID %s: storing user stats " % uuid + "*" * 10)
            print(str(arrow.now()) + "*" * 10 + "UUID %s: storing user stats " % uuid + "*" * 10)
            eaurs.get_and_store_pipeline_dependent_user_stats(uuid, "analysis/composite_trip")

        esds.store_pipeline_time(uuid, 'STORE_PIPELINE_DEPENDENT_USER_STATS',
                                time.time(), gsr.elapsed)
