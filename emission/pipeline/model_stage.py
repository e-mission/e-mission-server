from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import sys
import logging
import emission.net.usercache.abstract_usercache_handler as euah
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.tour_model_queries as esdtmq
from emission.core.get_database import get_db, get_mode_db, get_section_db

import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.intake.cleaning.clean_and_resample as eaicr
import emission.analysis.classification.inference.mode as md
import emission.net.ext_service.habitica.executor as autocheck
import emission.analysis.classification.inference.mode as pipeline

from uuid import UUID


'''
Questions:

Are we training the model on all trips or just the ones from this user?
Are we retraining the whole model every time?
How to know if the section has never been predicted?
	is it quicker to check for a match in the prediction db or
	to automatically store all sections in the prediction db with a 0 for prediction type
	and then just see how many we have in there and then match them up with the section?


'''
def setup(self):
	pass


def run_model_pipeline(process_number, uuid_list):
	"""
	Run the modeling stage of the pipeline. 


	"""





	for uuid in uuid_list:
		if uuid is None:
			continue

		try: 
			run_model_pipeline_for_user(uuid)
		except Exception as e:
			print "dang flabbit failed on error %s" % e


def run_mode_inference_pipeline_for_user(uuid):

	MIP = ModeInferencePipeline() #I don't think its this simple
	

	allConfirmedTripsQuery = ModeInferencePipeline.getSectionQueryWithGroundTruth({'$ne': ''})
	#These are all the trips that have a confirmed mode. We will be training off of this.

	#(MIP.modeList, MIP.confirmedSections) = MIP.loadTrainingDataStep(allConfirmedTripsQuery)



	MIP.runPipeline()


def update_model_for_user(uuid, algorithm_id=1):

	MIP = pipeline.ModelInferencePipeline()
	MIP.runModelBuildingStage(uuid = uuid, algorithm_id= algorithm_id)
	#user.model = MIP.getModel

def predict_for_user(uuid, timerange, model=None):

	if model is None:
		pass
		#model = user.model
	MIP = pipeline.ModelInferencePipeline()
	MIP.runModeInferenceStage(uuid, timerange, model=model)


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
	mark_mode_inference_done_for_user(uuid, MIP.getLastTimestamp})


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

