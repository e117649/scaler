import unittest
from typing import Any, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from scaler.config.defaults import DEFAULT_WORKER_RESTART_BACKOFF_SECONDS, WORKER_STOP_TIMEOUT_SECONDS
from scaler.utility.exitcode import WORKER_EXIT_CODE_SCHEDULER_UNREACHABLE
from scaler.worker_manager.native.worker_manager import NativeWorkerProvisioner, NativeWorkerSupervisor

DRAIN_TIMEOUT_SECONDS = 20


def _make_provisioner(max_task_concurrency: int = -1) -> NativeWorkerProvisioner:
    config = MagicMock()
    config.worker_config.per_worker_capabilities.capabilities = {}
    config.worker_manager_config.max_task_concurrency = max_task_concurrency
    config.worker_manager_config.worker_manager_id = "test-wm"
    config.worker_manager_config.scale_down_cooldown_seconds = 0
    config.worker_manager_config.drain_timeout_seconds = DRAIN_TIMEOUT_SECONDS
    config.worker_type = "NAT"
    return NativeWorkerProvisioner(config)


def _make_request(task_concurrency: int, capabilities: dict) -> MagicMock:
    request = MagicMock()
    request.taskConcurrency = task_concurrency
    request.capabilities = [MagicMock(key=k, value=v) for k, v in capabilities.items()]
    return request


class _FakeWorker:
    """A worker process the test kills and finishes at will."""

    def __init__(self, index: int, drain_request, processing_tasks) -> None:
        self.identity = f"NAT|worker-{index}"
        self.pid = 3000 + index
        self.sentinel = index
        self.drain_request = drain_request
        self.processing_tasks = processing_tasks
        self.exitcode: Optional[int] = None
        self.killed = False

    def start(self) -> None:
        pass

    def is_alive(self) -> bool:
        return self.exitcode is None

    def join(self) -> None:
        pass

    def kill(self) -> None:
        self.killed = True
        self.exitcode = -9

    def terminate(self) -> None:
        self.exitcode = 0


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class TestNativeWorkerSupervisor(unittest.TestCase):
    def setUp(self) -> None:
        self.workers: List[_FakeWorker] = []
        self.clock = _Clock()
        self.clock_patch = patch("scaler.worker_manager.native.worker_manager.time.monotonic", self.clock)
        self.clock_patch.start()
        self.kill_patch = patch("scaler.worker_manager.native.worker_manager.os.kill")
        self.os_kill = self.kill_patch.start()
        self.supervisor = NativeWorkerSupervisor(self._create_worker, DRAIN_TIMEOUT_SECONDS)

    def tearDown(self) -> None:
        self.clock_patch.stop()
        self.kill_patch.stop()

    def _create_worker(self, drain_request: Any, processing_tasks: Any) -> Any:
        worker = _FakeWorker(len(self.workers), drain_request, processing_tasks)
        self.workers.append(worker)
        return worker

    def test_replaces_a_worker_that_dies_after_the_backoff(self) -> None:
        self.supervisor.set_target(2)
        self.supervisor.routine()
        self.assertEqual(len(self.workers), 2)

        self.workers[0].exitcode = -9
        self.supervisor.routine()
        self.assertEqual(len(self.workers), 2, "a replacement waits out the backoff")
        self.assertEqual(self.supervisor.active_count(), 1)

        self.clock.now += DEFAULT_WORKER_RESTART_BACKOFF_SECONDS + 0.1
        self.supervisor.routine()
        self.assertEqual(len(self.workers), 3)
        self.assertEqual(self.supervisor.active_count(), 2)

    def test_backoff_doubles_with_each_consecutive_death(self) -> None:
        self.supervisor.set_target(1)
        self.supervisor.routine()

        for expected_backoff in (DEFAULT_WORKER_RESTART_BACKOFF_SECONDS, 2 * DEFAULT_WORKER_RESTART_BACKOFF_SECONDS):
            self.workers[-1].exitcode = 1
            started = len(self.workers)
            self.supervisor.routine()
            self.clock.now += expected_backoff - 0.1
            self.supervisor.routine()
            self.assertEqual(len(self.workers), started, f"no replacement before {expected_backoff}s")
            self.clock.now += 0.2
            self.supervisor.routine()
            self.assertEqual(len(self.workers), started + 1)

    def test_a_worker_that_exits_cleanly_gives_up_its_place(self) -> None:
        """Only an asked-for stop ends in 0, such as a client shutdown: the worker is not replaced."""
        self.supervisor.set_target(2)
        self.supervisor.routine()

        self.workers[0].exitcode = 0
        self.clock.now += 60
        self.supervisor.routine()
        self.assertEqual(len(self.workers), 2)

        self.workers[1].exitcode = 0
        self.supervisor.routine()
        self.assertTrue(self.supervisor.is_done())

    def test_a_worker_that_gave_up_on_the_scheduler_is_not_replaced(self) -> None:
        """A replacement would not reach the scheduler either, so the manager exits once every worker gave up."""
        self.supervisor.set_target(2)
        self.supervisor.routine()

        for worker in self.workers:
            worker.exitcode = WORKER_EXIT_CODE_SCHEDULER_UNREACHABLE
        self.clock.now += 60
        self.supervisor.routine()

        self.assertEqual(len(self.workers), 2)
        self.assertTrue(self.supervisor.is_done())

    def test_sheds_the_idlest_worker_first(self) -> None:
        self.supervisor.set_target(3)
        self.supervisor.routine()
        self.workers[0].processing_tasks.value = 1
        self.workers[1].processing_tasks.value = 0
        self.workers[2].processing_tasks.value = 1

        self.supervisor.set_target(2)
        self.supervisor.routine()

        self.assertEqual([worker.drain_request.is_set() for worker in self.workers], [False, True, False])
        self.assertEqual(self.supervisor.active_count(), 2)

    def test_drain_all_drains_then_stops_then_kills(self) -> None:
        """SIGTERM drains; a drain past its deadline becomes a stop; a stop past its deadline becomes a kill."""
        self.supervisor.set_target(2)
        self.supervisor.routine()
        self.workers[0].processing_tasks.value = 1

        self.supervisor.drain_all()
        self.assertTrue(all(worker.drain_request.is_set() for worker in self.workers))

        self.workers[1].exitcode = 0  # finished its task in time
        self.supervisor.routine()
        self.os_kill.assert_not_called()

        self.clock.now += DRAIN_TIMEOUT_SECONDS + 0.1
        self.supervisor.routine()
        self.assertEqual(self.os_kill.call_count, 1, "the worker still running at the deadline is told to stop")

        self.clock.now += WORKER_STOP_TIMEOUT_SECONDS + 0.1
        self.supervisor.routine()
        self.assertTrue(self.workers[0].killed)

        self.supervisor.routine()
        self.assertTrue(self.supervisor.is_done())

    def test_starts_nothing_once_shutting_down(self) -> None:
        self.supervisor.set_target(1)
        self.supervisor.routine()
        self.supervisor.drain_all()
        self.workers[0].exitcode = -9
        self.clock.now += 60
        self.supervisor.routine()
        self.assertEqual(len(self.workers), 1)
        self.assertTrue(self.supervisor.is_done())


class TestNativeWorkerProvisionerConcurrencyConversion(unittest.IsolatedAsyncioTestCase):
    async def test_passes_task_concurrency_directly_as_desired_unit_count(self) -> None:
        provisioner = _make_provisioner()
        request = _make_request(task_concurrency=3, capabilities={})
        with patch.object(provisioner._capacity_coordinator, "_reconcile", new_callable=AsyncMock):
            await provisioner.set_desired_task_concurrency([request])
        self.assertEqual(provisioner._capacity_coordinator._desired_unit_count, 3)


class TestNativeWorkerProvisionerStopUnits(unittest.IsolatedAsyncioTestCase):
    async def test_stop_units_more_than_available_drains_them_all(self) -> None:
        provisioner = _make_provisioner()
        workers: List[_FakeWorker] = []

        def create_worker(drain_request: Any, processing_tasks: Any) -> Any:
            workers.append(_FakeWorker(len(workers), drain_request, processing_tasks))
            return workers[-1]

        provisioner._supervisor = NativeWorkerSupervisor(create_worker, DRAIN_TIMEOUT_SECONDS)
        await provisioner.start_units(2)
        await provisioner.stop_units(5)

        self.assertEqual(provisioner.active_unit_count(), 0)
        self.assertTrue(all(worker.drain_request.is_set() for worker in workers))
