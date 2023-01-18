import emission.core.get_database as edb

print("-" * 10, "inputs", "-" * 10)
print("_" * 10, "manual/mode_confirm", "_" * 10)
print(edb.get_timeseries_db().find({"metadata.key": "manual/mode_confirm"}).distinct("data.label"))
print("_" * 10, "analysis/confirmed_trip", "_" * 10)
print(edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/confirmed_trip"}).distinct("data.user_input.mode_confirm"))
print("-" * 10, "inferred_labels", "-" * 10)
def unique_labels_from_entries(label_prediction_entries):
    all_labels = []
    for lpe in label_prediction_entries:
        # print(lpe)
        for label_opt in lpe:
            # print(label_opt)
            if "mode_confirm" in label_opt["labels"]:
                all_labels.append(label_opt["labels"]["mode_confirm"])
    # print(all_labels)
    return list(set(all_labels))
print("_" * 10, "label predictions", "_" * 10)
print(unique_labels_from_entries([e["data"]["prediction"] for e in edb.get_analysis_timeseries_db().find({"metadata.key": "inference/labels"},{"data.prediction": 1, "_id": 0})]))
print("_" * 10, "inferred trips", "_" * 10)
print(unique_labels_from_entries([e["data"]["inferred_labels"] for e in edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/inferred_trip"},{"data.inferred_labels": 1, "_id": 0})]))
print("_" * 10, "expected trips", "_" * 10)
print(unique_labels_from_entries([e["data"]["inferred_labels"] for e in edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/expected_trip"},{"data.inferred_labels": 1, "_id": 0})]))
print("_" * 10, "confirmed trips", "_" * 10)
print(unique_labels_from_entries([e["data"]["inferred_labels"] for e in edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/confirmed_trip"},{"data.inferred_labels": 1, "_id": 0}) if "inferred_labels" in e["data"]]))
# print([e for e in edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/confirmed_trip"}) if "inferred_labels" not in e["data"]])
