import emission.core.get_database as edb

edb.get_timeseries_db().remove({'metadata.key': 'background/battery',
                                'data.battery_level_pct': -100})
