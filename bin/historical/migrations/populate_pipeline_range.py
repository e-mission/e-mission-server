import logging
import arrow
import argparse

import emission.core.get_database as edb
import emission.core.wrapper.entry as ecwe

import emission.pipeline.scheduler as eps
import emission.pipeline.intake_stage as epi

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.user_queries as esdu
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.analysis.plotting.composite_trip_creation as eapc
import emission.analysis.userinput.matcher as eaum

def add_pipeline_range(process_number, uuid_list, skip_if_no_new_data):
    import logging
    logging.basicConfig(level=logging.DEBUG, filename="/var/tmp/populate_pipeline_range_%s.log" % process_number,
        force=True)
    logging.info("processing UUID list = %s" % uuid_list)

    for uuid in uuid_list:
        if uuid is None:
            continue

        try:
            epi._get_and_store_range(uuid, "analysis/composite_trip")
        except Exception as e:
            print("Found error %s while processing pipeline for user %s, check log files for details"
                % (e, uuid))
            logging.exception("Found error %s while processing pipeline "
                              "for user %s, skipping" % (e, uuid))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("n_workers", type=int,
                        help="the number of worker processors to use")
    args = parser.parse_args()
    split_lists = eps.get_split_uuid_lists(args.n_workers)
    logging.info("Finished generating split lists %s" % split_lists)
    eps.dispatch(split_lists, target_fn=add_pipeline_range)

