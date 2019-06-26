from limesurveyrc2api.limesurvey import LimeSurvey
import json
import logging
import argparse
import uuid

import emission.net.ext_service.push.notify_usage as pnu
import emission.net.ext_service.push.query.dispatch as pqd
import emission.core.wrapper.user as ecwu
import emission.net.ext_service.limesurvey as LimeSurvey

url = "https://loicmayol.limequery.com/admin/remotecontrol"
username = "loicmayol"
password = "Sling13090"

print(ecwu.User.fromEmail("loicmayol@gmail.com").uuid)
print(ecwu.User.fromUUID(ecwu.User.fromEmail("loicmayol@gmail.com").uuid)._User__email)

print(LimeSurvey.get_all_surveys())