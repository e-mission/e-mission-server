import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import emission.core.deployment_config as dc

class TestConfigLoad(unittest.TestCase):
    def tearDown(self):
        """Reset global deployment config after each test to prevent interference."""
        dc.deployment_config = None

    def test_load_default_label_options_reads_resource(self):
        expected = {"mode": ["walk", "bike"]}

        async def fake_read_json_resource(name):
            self.assertEqual(name, "label-options.default.json")
            return expected

        with patch.object(dc.emcu, "read_json_resource", new=fake_read_json_resource):
            result = asyncio.run(dc._load_default_label_options())
            self.assertEqual(result, expected)

    def test_load_label_options_uses_dynamic_labels(self):
        dynamic_labels = {"purpose": ["work", "school"]}
        base_config = {"label_options": "https://example.com/labels.json"}

        async def fake_fetch_url(url):
            self.assertEqual(url, "https://example.com/labels.json")
            return SimpleNamespace(text=json.dumps(dynamic_labels))

        with patch.object(dc.emcu, "fetch_url", new=fake_fetch_url):
            asyncio.run(dc._load_label_options(base_config, "test-study"))
            self.assertEqual(base_config["label_options"], dynamic_labels)

    def test_load_label_options_falls_back_to_default(self):
        default_labels = {"replaced_mode": ["bus", "train"]}
        base_config = {}

        async def fake_load_default_label_options():
            return default_labels

        with patch.object(dc, "_load_default_label_options", new=fake_load_default_label_options):
            asyncio.run(dc._load_label_options(base_config, "test-study"))
            self.assertEqual(base_config["label_options"], default_labels)

    def test_load_label_options_handles_nonexistent_dynamic_url(self):
        """Test that failed dynamic-label fetch stores empty labels without raising."""
        base_config = {
            "label_options": "https://example.com/labels.json"
        }

        with patch.object(dc.emcu, "fetch_url", side_effect=ConnectionError("mock network error")):
            asyncio.run(dc._load_label_options(base_config, "test-study"))
            self.assertEqual(base_config["label_options"], {})

    def test_load_supplemental_files_waits_for_tasks(self):
        base_config = {}
        called = {"value": False}

        async def fake_load_label_options(base_config_arg, study_config_arg):
            called["value"] = True
            base_config_arg["label_options"] = {"ok": True}

        with patch.object(dc, "_load_label_options", new=fake_load_label_options):
            asyncio.run(dc._load_supplemental_files(base_config, "test-study", "https://configs/"))

        self.assertTrue(called["value"])
        self.assertEqual(base_config["label_options"], {"ok": True})

    def test_cache_respect_different_study_configs_after_reset(self):
        """Test that different study configs do not return wrong cached result."""
        config1 = {
            "version": "1.0",
            "name": "config1"
        }
        config2 = {
            "version": "2.0",
            "name": "config2"
        }

        def fake_get_base_config(study_config, configs_url, default):
            if study_config == "study1":
                return config1.copy()
            elif study_config == "study2":
                return config2.copy()
            return default

        async def fake_load_supplemental_files(base_config, study_config, configs_url):
            base_config["label_options"] = {"ok": True}

        with patch.object(dc, "_get_base_config", new=fake_get_base_config):
            with patch.object(dc, "_load_supplemental_files", new=fake_load_supplemental_files):
                # Reset global cache before test
                dc.deployment_config = None

                # Call with study1
                result1 = dc.get_deployment_config("study1", "https://configs/")
                self.assertEqual(result1["name"], "config1")

                # Reset cache to simulate separate invocation
                dc.deployment_config = None

                # Call with study2 - should get config2, not cached config1
                result2 = dc.get_deployment_config("study2", "https://configs/")
                self.assertEqual(result2["name"], "config2")

    def test_caching_prevents_repeated_network_calls(self):
        """Test that deployment config is cached and not re-fetched on second call."""
        base_config = {
            "version": "1.0",
            "name": "cached-config"
        }

        call_counts = {"get_base_config": 0, "load_supplemental": 0}

        def fake_get_base_config(study_config, configs_url, default):
            call_counts["get_base_config"] += 1
            return base_config.copy()

        async def fake_load_supplemental_files(base_config_arg, study_config, configs_url):
            call_counts["load_supplemental"] += 1
            base_config_arg["label_options"] = {"ok": True}

        with patch.object(dc, "_get_base_config", new=fake_get_base_config):
            with patch.object(dc, "_load_supplemental_files", new=fake_load_supplemental_files):
                # Reset global cache before test
                dc.deployment_config = None

                # First call should fetch from network
                result1 = dc.get_deployment_config("test-study", "https://configs/")
                self.assertEqual(call_counts["get_base_config"], 1)
                self.assertEqual(call_counts["load_supplemental"], 1)
                self.assertEqual(result1["name"], "cached-config")

                # Subsequent calls should return cached result without network calls
                for i in range(2, 6):
                    result = dc.get_deployment_config("test-study", "https://configs/")
                    self.assertEqual(call_counts["get_base_config"], 1, f"Call {i}: get_base_config should not be called again")
                    self.assertEqual(call_counts["load_supplemental"], 1, f"Call {i}: load_supplemental should not be called again")
                    self.assertEqual(result["name"], "cached-config")
                    self.assertIs(result1, result, f"Call {i}: Should return the exact same cached object")

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
