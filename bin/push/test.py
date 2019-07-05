from limesurveyrc2api.limesurvey import LimeSurvey
import json
import logging
import argparse
import uuid

import emission.net.ext_service.push.notify_usage as pnu
import emission.net.ext_service.push.query.dispatch as pqd
import emission.core.wrapper.user as ecwu
import emission.net.ext_service.limesurvey.limesurvey as LimeSurvey

essai = LimeSurvey.get_instance()

essai.open_api()
print(essai.get_surveys_user(ecwu.User.fromEmail("loicm@gmail.com").uuid))
essai.close_api()