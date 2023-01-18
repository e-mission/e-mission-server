import emission.core.get_database as edb

OLD_EBIKE_LABELS = ["pilot_ebike"]
NEW_EBIKE_LABEL = "e-bike"

print("-" * 10, "inputs", "-" * 10)
print("_" * 10, "manual/mode_confirm", "_" * 10)
print(edb.get_timeseries_db().update_many({"metadata.key": "manual/mode_confirm", "data.label": {"$in": OLD_EBIKE_LABELS}}, {"$set": {"data.label": "e-bike"}}).raw_result)
print("_" * 10, "analysis/confirmed_trip", "_" * 10)
print(edb.get_analysis_timeseries_db().update_many({"metadata.key": "analysis/confirmed_trip", "data.user_input.mode_confirm": {"$in": OLD_EBIKE_LABELS}}, {"$set": {"data.user_input.mode_confirm": "e-bike"}}).raw_result)
print("-" * 10, "inferred_labels", "-" * 10)

### See whether we have entries which need to be changed
def has_inferred_ebike_labels(lpe):
    for label_opt in lpe:
        if "mode_confirm" in label_opt["labels"] and label_opt["labels"]["mode_confirm"] in OLD_EBIKE_LABELS:
            return True
    return False

### Pass in only the entries which need to be changed
def fix_inferred_labels(field, tp):
    # print("Invoking with %s" % tp)
    idx = tp["_id"]
    lpe = tp["data"][field]
    for label_opt in lpe:
        # print(label_opt)
        if "mode_confirm" in label_opt["labels"] and label_opt["labels"]["mode_confirm"] in OLD_EBIKE_LABELS:
            label_opt["labels"]["mode_confirm"] = "e-bike"
            data_field = "data.%s" % field
            update_response = edb.get_analysis_timeseries_db().update_many({"_id": idx}, {"$set": {data_field: lpe}})
            if update_response.matched_count != 1 or update_response.modified_count != 1:
                print(f"Tried to update {idx}, got response {update_response.raw_result}")
            # print("Update response %s" % edb.get_analysis_timeseries_db().update_many({"_id": idx}, {"$set": {data_field: lpe}}))

def fix_label_field_if_needed(field, trip_projections):
    print(f"Fixing label fields for {len(trip_projections)} trips and the {field} field")
    fixed_entry_count = 0
    total_label_opt_count = 0
    for tp in trip_projections:
        # print("Checking tp %s" % tp)
        if field in tp["data"]:
            total_label_opt_count = total_label_opt_count + len(tp["data"][field])
            if has_inferred_ebike_labels(tp["data"][field]):
                # print("Fixing projection %s" % tp)
                fix_inferred_labels(field, tp)
                fixed_entry_count = fixed_entry_count + 1
                # print("Fixed projection %s" % tp)
    # print(f"Total label opt = {total_label_opt_count}")
    return "Fixed %s entries" % fixed_entry_count

print("_" * 10, "label predictions", "_" * 10)
print(fix_label_field_if_needed("prediction", list(edb.get_analysis_timeseries_db().find({"metadata.key": "inference/labels"},{"data.prediction": 1, "_id": 1}))))
print("_" * 10, "inferred trips", "_" * 10)
print(fix_label_field_if_needed("inferred_labels", list(edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/inferred_trip"},{"data.inferred_labels": 1, "_id": 1}))))
print("_" * 10, "expected trips", "_" * 10)
print(fix_label_field_if_needed("inferred_labels", list(edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/expected_trip"},{"data.inferred_labels": 1, "_id": 1}))))
print("_" * 10, "confirmed trips", "_" * 10)
print(fix_label_field_if_needed("inferred_labels", list(edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/confirmed_trip"},{"data.inferred_labels": 1, "_id": 1}))))
# print([e for e in edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/confirmed_trip"}) if "inferred_labels" not in e["data"]])
