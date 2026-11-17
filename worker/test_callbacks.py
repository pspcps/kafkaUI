"""Run with: python -m unittest worker.test_callbacks."""
import importlib.util
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch


class CallbackAuthTests(unittest.TestCase):
    def test_metrics_and_errors_send_worker_credential(self):
        for module_name, arguments in (
            ("metrics", ("job-1", 0, 1, 2, 3)),
            ("errors", ("job-1", 0, 1, "processing failed")),
        ):
            with self.subTest(module=module_name):
                requests = Mock()
                with patch.dict(sys.modules, {"requests": requests}), patch.dict(
                    os.environ,
                    {"WORKER_API_TOKEN": "service-credential", "MGMT_API_URL": "https://api.example.test"},
                ):
                    spec = importlib.util.spec_from_file_location(
                        f"callback_{module_name}", Path(__file__).with_name(f"{module_name}.py")
                    )
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    module.report(*arguments)
                    args, kwargs = requests.post.call_args
                    self.assertEqual(args[0], f"https://api.example.test/api/v1/jobs/job-1/{module_name}")
                    self.assertEqual(kwargs["headers"], {"Authorization": "Bearer service-credential"})
                    self.assertEqual(kwargs["timeout"], 5)
