import json
import unittest
import uuid
from unittest import mock

import emission.analysis.classification.inference.mode.rule_engine as erme
import emission.core.get_database as edb
import emission.pipeline.intake_stage as epi
import emission.net.ext_service.transit_matching.match_stops as enetm
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.tests.common as etc


class OverpassConsumptionTest(unittest.TestCase):
    def setUp(self):
        self.data_file = "emission/tests/data/real_examples/jack_untracked_time_2023-03-12"
        self.testEmail = f"overpass-pipeline-{uuid.uuid4()}@example.com"
        self.addCleanup(self.clearRelatedDb)
        etc.setupRealExample(self, self.data_file)

    def tearDown(self):
        self.doCleanups()

    def clearRelatedDb(self):
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

    def test_intake_pipeline_with_live_overpass(self):
        with open(self.data_file + ".inferred_section.expected_composite_trips") as expected_file:
            expected_trips = json.load(expected_file)
        expected_bus_intervals = [
            (section["data"]["start_ts"], section["data"]["end_ts"])
            for trip in expected_trips
            for section in trip["data"].get("sections", [])
            if section["data"].get("sensed_mode_str") == "BUS"
        ]
        self.assertTrue(expected_bus_intervals, "Ground truth contains no BUS sections")

        transit_diagnostics = []
        original_transit_prediction = erme._get_transit_prediction
        original_get_stops_near = enetm.get_stops_near
        original_get_predicted_transit_mode = enetm.get_predicted_transit_mode

        def summarize_stops(stops):
            route_tags = [
                route.get("tags", {})
                for stop in stops
                for route in stop.get("routes", [])
            ]
            return {
                "stop_count": len(stops),
                "bus_stop_count": sum(
                    stop.get("tags", {}).get("highway") == "bus_stop"
                    for stop in stops
                ),
                "route_ref_count": sum(
                    "route_ref" in stop.get("tags", {}) for stop in stops
                ),
                "attached_route_count": len(route_tags),
                "route_types": sorted({
                    tags["route"] for tags in route_tags if "route" in tags
                }),
                "railway_types": sorted({
                    stop.get("tags", {})["railway"]
                    for stop in stops
                    if "railway" in stop.get("tags", {})
                }),
            }

        def capture_transit_prediction(index, section_entry):
            stop_results = []
            candidate_modes = []

            def capture_stops_near(location, radius):
                stops = original_get_stops_near(location, radius)
                stop_results.append(summarize_stops(stops))
                return stops

            def capture_candidate_modes(start_stops, end_stops):
                modes = original_get_predicted_transit_mode(start_stops, end_stops)
                candidate_modes.append(modes)
                return modes

            collapsed_mode = None
            error_type = None
            try:
                with mock.patch.object(
                    enetm, "get_stops_near", side_effect=capture_stops_near
                ), mock.patch.object(
                    enetm,
                    "get_predicted_transit_mode",
                    side_effect=capture_candidate_modes,
                ):
                    collapsed_mode = original_transit_prediction(index, section_entry)
            except Exception as error:
                error_type = type(error).__name__
                raise
            finally:
                transit_diagnostics.append({
                    "interval": (
                        section_entry.data.start_ts,
                        section_entry.data.end_ts,
                    ),
                    "start_stops": stop_results[0] if stop_results else None,
                    "end_stops": stop_results[1] if len(stop_results) > 1 else None,
                    "candidate_modes": candidate_modes[0] if candidate_modes else None,
                    "collapsed_mode": collapsed_mode,
                    "error_type": error_type,
                })
            return collapsed_mode

        with mock.patch.object(
            erme, "_get_transit_prediction", side_effect=capture_transit_prediction
        ):
            epi.run_intake_pipeline_for_user(self.testUUID)

        predictions = esda.get_entries(
            "inference/prediction", self.testUUID, time_query=None)
        prediction_by_interval = {
            (prediction.data.start_ts, prediction.data.end_ts): prediction
            for prediction in predictions
        }
        diagnostic_by_interval = {
            diagnostic["interval"]: diagnostic
            for diagnostic in transit_diagnostics
        }
        # The fixture records historical labels, while Overpass reflects current
        # OSM data. Require the pipeline to query and consume live results without
        # treating changes to the map as pipeline regressions.
        for interval in expected_bus_intervals:
            diagnostic = diagnostic_by_interval.get(interval)
            self.assertIn(
                interval, prediction_by_interval,
                f"Expected live pipeline to save a prediction for interval "
                f"{interval}; transit diagnostics: {diagnostic}")
            self.assertIsNotNone(
                diagnostic,
                f"Expected live pipeline to query transit data for interval "
                f"{interval}")
            self.assertIsNone(
                diagnostic["error_type"],
                f"Expected live transit inference to complete for interval "
                f"{interval}; transit diagnostics: {diagnostic}")
            if diagnostic["collapsed_mode"] is not None:
                self.assertIn(
                    diagnostic["collapsed_mode"],
                    prediction_by_interval[interval].data["predicted_mode_map"],
                    f"Expected prediction to consume live transit inference for "
                    f"interval {interval}; transit diagnostics: {diagnostic}")

if __name__ == '__main__':
    unittest.main()
