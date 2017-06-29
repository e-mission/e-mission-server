import sys
import logging
import emission.net.usercache.abstract_usercache_handler as euah
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.tour_model_queries as esdtmq

import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.intake.cleaning.clean_and_resample as eaicr
import emission.net.ext_service.habitica.executor as autocheck
import emission.analysis.classification.inference.mode as pipeline

from uuid import UUID

def run_mode_pipeline(process_number, uuid_list):

	MIP = pipeline.ModelInferencePipeline()
	for user_id in uuid_list:
		try:
			run_mode_pipeline_for_user(MIP, user_id)
		except e:
			logging.debug("Skipping user %s because of error" % user_id)
			logging.debug("error %s" % e)
			mark_mode_failed_for_user(user_id)

def run_mode_pipeline_for_user(MIP, uuid):

	timerange = get_time_range_for_mode_inference(uuid)
	MIP.runPipelineModelStage(uuid, timerange)
	mark_mode_done_for_user(uuid, timerange) # I know this timerange should be something else, but I'm not sure what.
	#											maybe the most recent end ts grabbed? In that case, I may need to return that from runPipelineModelStage

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                        level=logging.DEBUG)

    long_term_uuid_list = esta.TimeSeries.get_uuid_list()
    logging.info("*" * 10 + "long term UUID list = %s" % long_term_uuid_list)
    for uuid in long_term_uuid_list:
        if uuid is None:
            continue

        logging.info("*" * 10 + "UUID %s: finding common trips" % uuid + "*" * 10)
        esdtmq.make_tour_model_from_raw_user_data(uuid)

