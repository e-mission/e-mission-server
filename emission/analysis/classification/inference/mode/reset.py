import logging

# Delete the objects created by this pipeline step (across users)
def del_all_objects(is_dry_run):
    del_query = {}
    del_query.update({"metadata.key": {"$in": ["inference/prediction", "analysis/inferred_section"]}})
    logging.info("About to delete %d entries" 
        % edb.get_analysis_timeseries_db().find(del_query).count())
    logging.info("About to delete entries with keys %s" 
        % edb.get_analysis_timeseries_db().find(del_query).distinct("metadata.key"))

    del_pipeline_query = {"pipeline_stage": ecwp.PipelineStages.MODE_INFERENCE.value}
    logging.info("About to delete pipeline entries for stage %s" %
        ecwp.PipelineStages.MODE_INFERENCE)

    if is_dry_run:
        logging.info("this is a dry-run, returning from del_objects_after without modifying anything")
    else:
        result = edb.get_analysis_timeseries_db().delete_many(del_query)
        logging.info("this is not a dry-run, result of deleting analysis entries is %s" % result.raw_result)
        result = edb.get_pipeline_state_db().delete_many(del_pipeline_query)
        logging.info("this is not a dry-run, result of deleting pipeline state is %s" % result.raw_result)

# Delete the objects created by this pipeline step (for a particular user)
def del_objects_after(user_id, reset_ts, is_dry_run):
    del_query = {}
    # handle the user
    del_query.update({"user_id": user_id})

    del_query.update({"metadata.key": {"$in": ["inference/prediction", "analysis/inferred_section"]}})
    # all objects inserted here have start_ts and end_ts and are trip-like
    del_query.update({"data.start_ts": {"$gt": reset_ts}})
    logging.debug("After all updates, del_query = %s" % del_query)

    reset_pipeline_query = {"pipeline_stage": ecwp.PipelineStages.MODE_INFERENCE.value}
    # Fuzz the TRIP_SEGMENTATION stage 5 mins because of
    # https://github.com/e-mission/e-mission-server/issues/333#issuecomment-312730217
    FUZZ_FACTOR = 5 * 60
    reset_pipeline_update = {'$set': {'last_processed_ts': reset_ts + FUZZ_FACTOR}}
    logging.info("About to reset stage %s to %s" 
        % (ecwp.PipelineStages.MODE_INFERENCE, reset_ts))
    

    logging.info("About to delete %d entries" 
        % edb.get_analysis_timeseries_db().find(del_query).count())
    logging.info("About to delete entries with keys %s" 
        % edb.get_analysis_timeseries_db().find(del_query).distinct("metadata.key"))
    
    if is_dry_run:
        logging.info("this is a dry-run, returning from del_objects_after without modifying anything")
    else:
        result = edb.get_analysis_timeseries_db().remove(del_query)
        logging.info("this is not a dry-run, result of deleting analysis entries is %s" % result)

