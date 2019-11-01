import emission.core.wrapper.tier as st
import emission.core.get_database as edb
import pandas as pd
from uuid import UUID
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt
import emission.core.wrapper.motionactivity as ecwm
import arrow
from emission.core.get_database import get_similar_trips_db

class SimilarTrips:

    def __init__(self, max_distance = 10, interval = 1):
        self.levels = []
        for i in range(1, max_distance + 1):
            self.levels.append([])

    def addTrip(self):
        

    def get_similar_distance_trip(self, ):
