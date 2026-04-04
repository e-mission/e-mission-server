# For debugging DB locally
# Record a stat every time the DB is queried by monitoring the MongoDB client


import inspect
import time
from pymongo.monitoring import register, CommandListener


class QueryMonitor(CommandListener):
    def started(self, event):
        event_cmd = str(event.command)
        if ('stats/pipeline_time' not in event_cmd):
            call_stack = [f.function for f in inspect.stack()][11:]
            try:
                import emission.storage.decorations.stats_queries as esds
                esds.store_pipeline_time(None,
                                         f'db_call/{event.command_name}',
                                         time.time(),
                                         str(call_stack))
            except AttributeError:
                # timeseries not initialized yet, skip
                pass

    def __init__(self): pass
    def succeeded(self, _): pass
    def failed(self, _): pass


def register_db_query_monitor():
    print("Registering DB query monitor")
    register(QueryMonitor())
