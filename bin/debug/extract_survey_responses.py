# This will become obsolete in the future when we have an NREL hosted instance
# and store surveys in-house, presumably using enketo toolbox or survey.js
# but we may choose to use a standard survey tool instead as well if approved by cyber
# regardless, for the CEO e-bike survey, we currently have all survey responses
# lumped together into the same form responses, and we need to pull out the responses by group

import sys
import logging
logging.basicConfig(level=logging.DEBUG)
import gzip

import uuid
import datetime as pydt
import json
import bson.json_util as bju
import arrow
import argparse

import emission.core.wrapper.user as ecwu
import emission.storage.decorations.user_queries as esdu

import pandas as pd

def extract_survey_responses(uuid_list, in_fp, out_fp):
    all_responses = pd.read_csv(in_fp)
    all_responses.rename(columns = {"Unique User ID (auto-filled, do not edit)": "uuid"}, inplace=True)
    all_responses.rename(columns = {"Número único de usuario (autocompletado, no editar)": "uuid"}, inplace=True)
    is_program_uuid = []
    for ustr in all_responses.to_dict(orient='list')["uuid"]:
        try:
            is_program_uuid.append(uuid.UUID(ustr) in uuid_list)
        except Exception as e:
            logging.error("Badly formed UUID string %s" % ustr)
            is_program_uuid.append(False)
    program_responses = all_responses[is_program_uuid]
    program_responses.to_csv(out_fp)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(prog="extract_survey_responses")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-e", "--user_email", nargs="+")
    group.add_argument("-u", "--user_uuid", nargs="+")
    group.add_argument("-a", "--all", action="store_true")
    group.add_argument("-f", "--file")

    parser.add_argument("all_survey_response_csv")
    parser.add_argument("-o", "--output")

    args = parser.parse_args()

    if args.user_uuid:
        uuid_list = [uuid.UUID(uuid_str) for uuid_str in args.user_uuid]
    elif args.user_email:
        uuid_list = [ecwu.User.fromEmail(uuid_str).uuid for uuid_str in args.user_email]
    elif args.all:
        uuid_list = esdu.get_all_uuids()
    elif args.file:
        with open(args.file) as fd:
            uuid_entries = json.load(fd, object_hook=bju.object_hook)
            uuid_list = [ue["uuid"] for ue in uuid_entries]

    in_fp = open(args.all_survey_response_csv)

    if (args.output):
        out_fp = open(args.output, "w")
    else:
        out_fp = sys.stdout

    extract_survey_responses(uuid_list, in_fp, out_fp)
