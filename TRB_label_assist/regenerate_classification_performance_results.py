import pandas as pd
import numpy as np
from uuid import UUID

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.trip_queries as esdtq

from performance_eval import get_clf_metrics, cv_for_all_algs, PREDICTORS

import logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")

all_users = esta.TimeSeries.get_uuid_list()
confirmed_trip_df_map = {}
labeled_trip_df_map = {}
expanded_labeled_trip_df_map = {}
expanded_all_trip_df_map = {}
for u in all_users:
    ts = esta.TimeSeries.get_time_series(u)
    ct_df = ts.get_data_df("analysis/confirmed_trip")

    confirmed_trip_df_map[u] = ct_df
    labeled_trip_df_map[u] = esdtq.filter_labeled_trips(ct_df)
    expanded_labeled_trip_df_map[u] = esdtq.expand_userinputs(
        labeled_trip_df_map[u])
    expanded_all_trip_df_map[u] = esdtq.expand_userinputs(
        confirmed_trip_df_map[u])

n_trips_df = pd.DataFrame(
    [[u, len(confirmed_trip_df_map[u]),
      len(labeled_trip_df_map[u])] for u in all_users],
    columns=["user_id", "all_trips", "labeled_trips"])

all_trips = n_trips_df.all_trips.sum()
labeled_trips = n_trips_df.labeled_trips.sum()
unlabeled_trips = all_trips - labeled_trips
print('RESULT: {} ({:.2f}%) unlabeled, {} ({:.2f}%) labeled, {} total trips'.format(
    unlabeled_trips, unlabeled_trips / all_trips, labeled_trips,
    labeled_trips / all_trips, all_trips))

n_users_too_few_trips = len(n_trips_df[n_trips_df.labeled_trips < 5])
print(
    'RESULT: {}/{} ({:.2f}%) users have less than 5 labeled trips and cannot do cross-validation'
    .format(n_users_too_few_trips, len(n_trips_df),
            n_users_too_few_trips / len(n_trips_df)))

# load in all runs
model_names = list(PREDICTORS.keys())
cv_results = cv_for_all_algs(
    uuid_list=all_users,
    expanded_trip_df_map=expanded_labeled_trip_df_map,
    model_names=model_names,
    override_prior_runs=True,
    raise_errors=False,
    random_state=42,
)

