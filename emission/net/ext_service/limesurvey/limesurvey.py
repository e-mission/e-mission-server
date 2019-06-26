from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import json
import logging
import importlib

# Our imports
from limesurveyrc2api.limesurvey import LimeSurvey
import emission.core.wrapper.user as ecwu

try:
    limesurvey_config_file = open('conf/net/ext_service/limesurvey.json')
    limesurvey_config = json.load(limesurvey_config_file)
except:
    logging.warning("LimeSurvey service not configured, LimeSurvey not supported")

def get_instance():
    return Limesurvey(limesurvey_config)

class Limesurvey(object):
    def __init__(self, limesurvey_config):
        self.url = limesurvey_config["url"]
        self.username =  limesurvey_config["username"]
        self.password =  limesurvey_config["password"]

    def open_api(self):
        self.api = LimeSurvey(url=self.url, username=self.username)
        self.api.open(password=self.password)
    
    def close_api(self):
        session_key_released = self.api.close()
    
    def get_all_surveys(self):
        result = self.api.survey.list_surveys()
        return result

    def add_participants_by_mail(self, survey_id, email_list):
        participant_list = []
        for email in email_list:
            token = ecwu.User.fromEmail(email).uuid.hex
            participant_list.append({"email": email, "token": token})
        
        response = self.api.token.add_participants(survey_id=survey_id, 
                                                participant_data=participant_list,
                                                create_token_key=False)
        if "status" in response:
            if response["status"] == "No survey participants table":
                logging.warning("The survey %d as not been configured correctly, please read the doc for more informations" % survey_id)
            
        return response
    
    def add_participants_by_uuid(self, survey_id, uuid_list):
        participant_list = []
        for uuid in uuid_list:
            token = uuid.hex
            email = ecwu.User.fromUUID(uuid)._User__email
            participant_list.append({"email": email, "token": token})
        
        response = self.api.token.add_participants(survey_id=survey_id, 
                                                participant_data=participant_list,
                                                create_token_key=False)
        if "status" in response:
            if response["status"] == "No survey participants table":
                logging.warning("The survey %d as not been configured correctly, please read the doc for more informations" % survey_id)
            
        return response

    def get_participants_not_answered(self, survey_id):
        try:
            participant_list = self.api.token.list_participants(survey_id=survey_id, conditions={"completed":False})
            email_list = []
            for participant in participant_list:
                participant_info = participant["participant_info"]
                email_list.append(participant_info["email"])
            return email_list
        except:
            response = self.api.token.get_summary(survey_id=survey_id)
            if response["token_count"] == response["token_completed"]:
                logging.warning("Every participants as answered to %d" % survey_id)
            else:
                logging.warning("No participants found in %d" % survey_id)
            return []