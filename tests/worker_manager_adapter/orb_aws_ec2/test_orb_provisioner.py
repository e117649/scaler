import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scaler.worker_manager_adapter.common import extract_desired_count
from scaler.worker_manager_adapter.orb_aws_ec2.worker_manager import ORBWorkerProvisioner


def _make_provisioner(workers_per_instance: int = 1) -> ORBWorkerProvisioner:
    config = MagicMock()
    config.worker_config.per_worker_capabilities.capabilities = {"cpu": 4}
    sdk = MagicMock()
    return ORBWorkerProvisioner(
        config=config, max_instances=-1, sdk=sdk, template_id="tmpl-123", workers_per_instance=workers_per_instance
    )


class TestORBWorkerProvisionerReconcile(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.provisioner = _make_provisioner()

    async def test_reconcile_increases_worker_count(self) -> None:
        self.provisioner._desired_count = 3
        with patch.object(self.provisioner, "start_units", new_callable=AsyncMock) as start_mock:
            with patch.object(self.provisioner, "stop_units", new_callable=AsyncMock) as stop_mock:
                await self.provisioner._reconcile()
                start_mock.assert_called_once_with(3)
                stop_mock.assert_not_called()

    async def test_reconcile_decreases_worker_count(self) -> None:
        self.provisioner._units = ["i-1", "i-2", "i-3"]
        self.provisioner._desired_count = 1
        with patch.object(self.provisioner, "start_units", new_callable=AsyncMock) as start_mock:
            with patch.object(self.provisioner, "stop_units", new_callable=AsyncMock) as stop_mock:
                await self.provisioner._reconcile()
                start_mock.assert_not_called()
                stop_mock.assert_called_once_with(2)

    async def test_set_desired_task_concurrency_triggers_reconcile(self) -> None:
        request = MagicMock()
        request.taskConcurrency = 3
        request.capabilities = [MagicMock(key="cpu", value=4)]

        with patch.object(self.provisioner, "_reconcile", new_callable=AsyncMock) as reconcile_mock:
            await self.provisioner.set_desired_task_concurrency([request])
            self.assertIsNotNone(self.provisioner._pending_reconcile_task)
            await asyncio.sleep(0)

        self.assertEqual(self.provisioner._desired_count, 3)  # ceil(3 / 1) = 3
        reconcile_mock.assert_called_once()

    async def test_set_desired_task_concurrency_converts_workers_to_instances(self) -> None:
        provisioner = _make_provisioner(workers_per_instance=16)
        request = MagicMock()
        request.taskConcurrency = 100
        request.capabilities = [MagicMock(key="cpu", value=4)]

        with patch.object(provisioner, "_reconcile", new_callable=AsyncMock):
            await provisioner.set_desired_task_concurrency([request])
            await asyncio.sleep(0)

        self.assertEqual(provisioner._desired_count, 7)  # ceil(100 / 16) = 7

    async def test_set_desired_task_concurrency_coalesces_rapid_calls(self) -> None:
        request = MagicMock()
        request.taskConcurrency = 5
        request.capabilities = [MagicMock(key="cpu", value=4)]

        with patch.object(self.provisioner, "_reconcile", new_callable=AsyncMock) as reconcile_mock:
            await self.provisioner.set_desired_task_concurrency([request])
            await self.provisioner.set_desired_task_concurrency([request])
            await self.provisioner.set_desired_task_concurrency([request])
            await asyncio.sleep(0)

        reconcile_mock.assert_called_once()

    async def test_reconcile_no_change(self) -> None:
        self.provisioner._units = ["i-1"]
        self.provisioner._desired_count = 1
        with patch.object(self.provisioner, "start_units", new_callable=AsyncMock) as start_mock:
            with patch.object(self.provisioner, "stop_units", new_callable=AsyncMock) as stop_mock:
                await self.provisioner._reconcile()
                start_mock.assert_not_called()
                stop_mock.assert_not_called()


class TestExtractDesiredCount(unittest.TestCase):
    OWN_CAPABILITIES = {"cpu": 4}

    def _make_request(self, task_concurrency: int, capabilities: dict) -> MagicMock:
        request = MagicMock()
        request.taskConcurrency = task_concurrency
        request.capabilities = [MagicMock(key=key, value=value) for key, value in capabilities.items()]
        return request

    def test_returns_zero_for_empty_requests(self) -> None:
        self.assertEqual(extract_desired_count([], self.OWN_CAPABILITIES), 0)

    def test_exact_capability_match(self) -> None:
        request = self._make_request(task_concurrency=8, capabilities={"cpu": 4})
        self.assertEqual(extract_desired_count([request], self.OWN_CAPABILITIES), 8)

    def test_empty_capabilities_matches_as_wildcard(self) -> None:
        request = self._make_request(task_concurrency=5, capabilities={})
        self.assertEqual(extract_desired_count([request], self.OWN_CAPABILITIES), 5)

    def test_sums_all_matching_requests(self) -> None:
        wildcard = self._make_request(task_concurrency=2, capabilities={})
        specific = self._make_request(task_concurrency=6, capabilities={"cpu": 4})
        self.assertEqual(extract_desired_count([wildcard, specific], self.OWN_CAPABILITIES), 8)

    def test_returns_zero_when_no_request_matches(self) -> None:
        request = self._make_request(task_concurrency=3, capabilities={"gpu": 1})
        self.assertEqual(extract_desired_count([request], self.OWN_CAPABILITIES), 0)

    def test_sums_multiple_matches_excluding_non_matching(self) -> None:
        wildcard = self._make_request(task_concurrency=4, capabilities={})
        matching = self._make_request(task_concurrency=3, capabilities={"cpu": 4})
        non_matching = self._make_request(task_concurrency=10, capabilities={"gpu": 1})
        self.assertEqual(
            extract_desired_count([wildcard, matching, non_matching], self.OWN_CAPABILITIES),
            7,  # 4 + 3; non_matching excluded
        )
