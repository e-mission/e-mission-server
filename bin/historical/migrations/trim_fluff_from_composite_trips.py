import emission.core.get_database as edb

from _common import run_on_all_deployments


def trim_fluff_from_composite_trips():
    """
    Trim unnecessary fields from composite trips in the analysis_timeseries_db.
    The shape of the remaining fields is unchanged.
    """
    print("Trimming fluff from composite trips")
    analysis_ts = edb.get_analysis_timeseries_db()
    for ct in analysis_ts.find({'metadata.key': 'analysis/composite_trip'}):
        # print(f"Trimming {ct['_id']}, {ct['data'].get('start_ts')} - {ct['data'].get('end_ts')}")
        for l in ct['data'].get('locations', []):
            trim_entry(l, {
                'metadata': [],
                'data': ['loc', 'ts'],
            })

        for s in ct['data'].get('sections', []):
            trim_entry(s, {
                'metadata': [],
                'data': ['start_ts', 'end_ts', 'sensed_mode', 'sensed_mode_str',
                         'ble_sensed_mode', 'distance', 'duration'],
            })

        for key in ['start_confirmed_place', 'end_confirmed_place']:
            trim_entry(ct['data'].get(key), {
                '_id': True,
                'metadata': ['key'],
                'data': ['enter_ts', 'exit_ts', 'location', 'duration',
                         'user_input', 'additions'],
            })

        analysis_ts.update_one(
            {'_id': ct['_id']},
            {'$set': {'data': ct['data']}}
        )


def trim_entry(entry, fields_to_keep):
    if entry is None:
        return
    for key in list(entry):
        if key not in fields_to_keep:
            del entry[key]
        elif isinstance(entry[key], dict) and isinstance(fields_to_keep, dict):
            trim_entry(entry[key], fields_to_keep[key])


run_on_all_deployments(trim_fluff_from_composite_trips)
