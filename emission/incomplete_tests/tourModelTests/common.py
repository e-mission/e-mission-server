from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import *
import logging
import geojson as gj
import uuid

import emission.core.wrapper.cleanedtrip as ecwct
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.cleanedplace as ecwcp
import emission.core.get_database as edb

import emission.analysis.modelling.tour_model.cluster_pipeline as cp
import emission.storage.timeseries.abstract_timeseries as esta

def _createTripEntry(self, start_ts, end_ts, start_loc, end_loc):
    t = ecwct.Cleanedtrip()
    sp = ecwcp.Cleanedplace()
    ep = ecwcp.Cleanedplace()

    t.start_ts = start_ts
    t.end_ts = end_ts

    if start_loc is not None:
        t.start_loc = gj.Point(start_loc)
        sp.location = t.start_loc

    if end_loc is not None:
        t.end_loc = gj.Point(end_loc)
        ep.location = t.end_loc

    sp.exit_ts = start_ts
    ep.enter_ts = end_ts

    spe = ecwe.Entry.create_entry(self.testUUID, "analysis/cleaned_place", sp, create_id=True)
    epe = ecwe.Entry.create_entry(self.testUUID, "analysis/cleaned_place", ep, create_id=True)
    t.start_place = spe.get_id()
    t.end_place = epe.get_id()
    te = ecwe.Entry.create_entry(self.testUUID, "analysis/cleaned_trip", t, create_id=True)
    self.ts.insert(spe)
    self.ts.insert(epe)
    self.ts.insert(te)
    return te

def _setup(self):
    self.data = cp.read_data()
    #if len(self.data) == 0:
    #    tg.create_fake_trips()
    #    self.data = cp.read_data(size=100)
    print('there are ' + str(len(self.data)))
    self.testUUID = uuid.uuid4()
    self.ts = esta.TimeSeries.get_time_series(self.testUUID)

def _tearDown(self):
    edb.get_timeseries_db().remove({'user_id': self.testUUID})
    edb.get_analysis_timeseries_db().remove({'user_id': self.testUUID})
