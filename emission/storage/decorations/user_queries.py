# Utility class to return useful user queries
import logging

import emission.core.get_database as edb
import emission.storage.timeseries.aggregate_timeseries as estag
from uuid import UUID

# Comment this out and comment in the stuff below in production
TEMP_HANDLED_PUBLIC_PHONES = []

#TEMP_HANDLED_PUBLIC_PHONES = [UUID("079e0f1a-c440-3d7c-b0e7-de160f748e35"), # iphone 1
#                         UUID("c76a0487-7e5a-3b17-a449-47be666b36f6"), # iphone 2
#                         UUID("c528bcd2-a88b-3e82-be62-ef4f2396967a"), # iphone 3
#                         UUID("95e70727-a04e-3e33-b7fe-34ab19194f8b"), # iphone 4
#                         UUID("e471711e-bd14-3dbe-80b6-9c7d92ecc296"), # android 1
#                         UUID("fd7b4c2e-2c8b-3bfa-94f0-d1e3ecbd5fb7"), # android 2
#                         UUID("86842c35-da28-32ed-a90e-2da6663c5c73"), # android 3
#                         UUID("3bc0f91f-7660-34a2-b005-5c399598a369")] # android 4



def get_all_uuids():
    all_uuids = [e["uuid"] for e in edb.get_uuid_db().find()]
    return all_uuids

def get_test_phone_uuids():
    all_uuids = get_all_uuids()
    sel_uuids = [u for u in all_uuids if u in estag.TEST_PHONE_IDS]
    # Add back the test phones for now so that we can test the data
	# collection changes before deploying them in the wild
    sel_uuids.extend(TEMP_HANDLED_PUBLIC_PHONES)
    return sel_uuids

def get_non_test_phone_uuids():
    all_uuids = get_all_uuids()
    sel_uuids = [u for u in all_uuids if u not in estag.TEST_PHONE_IDS]
    # Add back the test phones for now so that we can test the data
	# collection changes before deploying them in the wild
    sel_uuids.extend(TEMP_HANDLED_PUBLIC_PHONES)
    return sel_uuids
