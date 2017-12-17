from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import range
from builtins import *
import logging
import multiprocessing as mp
import pandas as pd
from uuid import UUID

import emission.core.get_database as edb
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.aggregate_timeseries as estag
import emission.storage.decorations.user_queries as esdu
import emission.pipeline.intake_stage as epi

def get_split_uuid_lists(n_splits):
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

    sel_uuids = esdu.get_all_uuids()
    sel_jobs = [(u, get_count(u)) for u in sel_uuids]
    # non_zero_jobs = [j for j in sel_jobs if j[1] !=0 ]
    # Not filtering for now
    non_zero_jobs = sel_jobs
    logging.debug("sel_uuids = %s, sel_jobs = %s, non_zero_jobs = %s" %
                  (len(sel_uuids), len(sel_jobs), len(non_zero_jobs)))

    non_zero_jobs_df = pd.DataFrame(non_zero_jobs, columns=['user_id', 'count']).sort_values(by="count")
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

def dispatch(split_lists):
    ctx = mp.get_context('spawn')
    process_list = []
    for i, uuid_list in enumerate(split_lists):
        logging.debug("Dispatching list %s" % uuid_list)
        pid = i
        p = ctx.Process(target=epi.run_intake_pipeline, args=(pid, uuid_list))
        logging.info("Created process %s to process %s list of size %s" %
                     (p, i, len(uuid_list)))
        p.start()
        process_list.append(p)



