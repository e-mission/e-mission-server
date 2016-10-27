import logging
import multiprocessing as mp
import pandas as pd
from uuid import UUID

import emission.core.get_database as edb
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.aggregate_timeseries as estag
import emission.pipeline.intake_stage as epi

TEMP_HANDLED_PUBLIC_PHONES = [UUID("079e0f1a-c440-3d7c-b0e7-de160f748e35"),
                         UUID("95e70727-a04e-3e33-b7fe-34ab19194f8b"),
                         UUID("fd7b4c2e-2c8b-3bfa-94f0-d1e3ecbd5fb7"),
                         UUID("3bc0f91f-7660-34a2-b005-5c399598a369")]

def get_split_uuid_lists(n_splits, is_public_pipeline):
    get_count = lambda u: enua.UserCache.getUserCache(u).getMessageCount()

    """
    This is the count of messages in the usercache. While we can use it for the
    scheduling, it is just a hint, because having a zero count here is no guarantee
    of zero count in the processing. And in particular, having a zero count here is
    no indication that there are no pending entries in the long-term cache. I think
    that's why we used to have the long-term cache and the user cache uuid list separately.

    That technique is no longer feasible because it requires coordination after the
    usercache is processed instead of parallelizing the entire pipeline.

    In general, this should be a pretty good hint, but I am not sure that it is perfect,
    and I am not ready to try this out weeks before the big deployment, with one day's
    testing. I also don't want to use UserCache.getMessage() since it returns the entire
    list instead of just the count. So we use the count as a hint and don't filter out
    users based on that.
    """

    all_uuids = [e["uuid"] for e in edb.get_uuid_db().find()]
    if is_public_pipeline:
        sel_uuids = [u for u in all_uuids if u in estag.TEST_PHONE_IDS]
    else:
        sel_uuids = [u for u in all_uuids if u not in estag.TEST_PHONE_IDS]
	# Add back the test phones for now so that we can test the data
	# collection changes before deploying them in the wild
	sel_uuids.extend(TEMP_HANDLED_PUBLIC_PHONES)

    sel_jobs = [(u, get_count(u)) for u in sel_uuids]
    # non_zero_jobs = [j for j in sel_jobs if j[1] !=0 ]
    # Not filtering for now
    non_zero_jobs = sel_jobs
    logging.debug("all_uuids = %s, sel_uuids = %s, sel_jobs = %s, non_zero_jobs = %s" %
                  (len(all_uuids), len(sel_uuids), len(sel_jobs), len(non_zero_jobs)))

    non_zero_jobs_df = pd.DataFrame(non_zero_jobs, columns=['user_id', 'count']).sort("count")
    ret_splits = []
    for i in range(0, n_splits):
        ret_splits.append([])

    col = 0
    for i, nzj in enumerate(non_zero_jobs_df.to_dict('records')):
        ret_splits[col].append(nzj['user_id'])
        col = col + 1
        if col == n_splits:
            logging.debug("reached n_splits, setting to zero")
            col = 0
    logging.debug("Split values are %s" % ret_splits)
    return ret_splits

def dispatch(split_lists, is_public_pipeline):
    process_list = []
    for i, uuid_list in enumerate(split_lists):
        logging.debug("Dispatching list %s" % uuid_list)
        pid = "public_%s" % i if is_public_pipeline else i
        p = mp.Process(target=epi.run_intake_pipeline, args=(pid, uuid_list))
        logging.info("Created process %s to process %s list of size %s" %
                     (p, i, len(uuid_list)))
        p.start()
        process_list.append(p)



