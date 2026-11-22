import unittest
import uuid
import json

class OverpassConsumptionTest(unittest.TestCase):
    def test_intake_pipeline_with_live_overpass(self):
        import emission.core.get_database as edb
        import emission.pipeline.intake_stage as epi
        import emission.storage.decorations.analysis_timeseries_queries as esda
        import emission.tests.common as etc

        data_file = "emission/tests/data/real_examples/jack_untracked_time_2023-03-12"
        with open(data_file + ".inferred_section.expected_composite_trips") as expected_file:
            expected_trips = json.load(expected_file)
        expected_bus_intervals = [
            (section["data"]["start_ts"], section["data"]["end_ts"])
            for trip in expected_trips
            for section in trip["data"].get("sections", [])
            if section["data"].get("sensed_mode_str") == "BUS"
        ]
        self.assertTrue(expected_bus_intervals, "Ground truth contains no BUS sections")

        self.testEmail = f"overpass-pipeline-{uuid.uuid4()}@example.com"
        try:
            etc.setupRealExample(self, data_file)
            epi.run_intake_pipeline_for_user(self.testUUID)

            predictions = esda.get_entries(
                "inference/prediction", self.testUUID, time_query=None)
            predicted_bus_intervals = {
                (prediction.data.start_ts, prediction.data.end_ts)
                for prediction in predictions
                if "BUS" in prediction.data.predicted_mode_map
            }
            for interval in expected_bus_intervals:
                self.assertIn(
                    interval, predicted_bus_intervals,
                    f"Expected live pipeline to infer BUS for interval {interval}")
        finally:
            if hasattr(self, "testUUID"):
                for collection in (
                    edb.get_timeseries_db(),
                    edb.get_analysis_timeseries_db(),
                    edb.get_profile_db(),
                    edb.get_usercache_db(),
                    edb.get_pipeline_state_db(),
                ):
                    collection.delete_many({"user_id": self.testUUID})
                edb.get_uuid_db().delete_one({"user_email": self.testEmail})

if __name__ == '__main__':
    unittest.main()
