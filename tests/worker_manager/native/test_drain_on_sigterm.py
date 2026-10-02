import multiprocessing
import os
import signal
import sys
import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.defaults import DEFAULT_LOGGING_PATHS
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.config.types.worker import WorkerCapabilities
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager.native.worker_manager import NativeWorkerManager
from tests.utility.utility import logging_test_name

_TASK_SECONDS = 4
_TASK_STARTS_WITHIN_SECONDS = 2
_DRAIN_TIMEOUT_SECONDS = 20


def sleep_and_report(seconds: float) -> int:
    time.sleep(seconds)
    return os.getpid()


@unittest.skipIf(sys.platform == "win32", "SIGTERM cannot be delivered to a spawn child on Windows")
class TestDrainOnSigterm(unittest.TestCase):
    """SIGTERM, which is how Kubernetes, ECS and EC2 stop a unit, lets the running task finish where it is."""

    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.combo = SchedulerClusterCombo(n_workers=0, event_loop="builtin")
        self.addCleanup(self.combo.shutdown)

        manager = NativeWorkerManager(
            NativeWorkerManagerConfig(
                worker_manager_config=WorkerManagerConfig(
                    scheduler_address=self.combo._address,
                    worker_manager_id="pod",
                    object_storage_address=self.combo._object_storage_address,
                    max_task_concurrency=1,
                    drain_timeout_seconds=_DRAIN_TIMEOUT_SECONDS,
                ),
                mode=NativeWorkerManagerMode.FIXED,
                worker_config=WorkerConfig(per_worker_capabilities=WorkerCapabilities({}), event_loop="builtin"),
                logging_config=LoggingConfig(paths=DEFAULT_LOGGING_PATHS),
            )
        )
        self.manager_process = multiprocessing.get_context("spawn").Process(target=manager.run)
        self.manager_process.start()
        self.addCleanup(self._kill_manager)

    def _kill_manager(self) -> None:
        if self.manager_process.is_alive():
            self.manager_process.kill()
        self.manager_process.join()

    def test_the_running_task_finishes_and_the_manager_exits(self) -> None:
        with Client(self.combo.get_address()) as client:
            future = client.submit(sleep_and_report, _TASK_SECONDS)
            time.sleep(_TASK_STARTS_WITHIN_SECONDS + 1)

            stopped_at = time.time()
            os.kill(self.manager_process.pid, signal.SIGTERM)

            processor_pid = future.result(timeout=_DRAIN_TIMEOUT_SECONDS)
            self.manager_process.join(timeout=_DRAIN_TIMEOUT_SECONDS)

        self.assertFalse(self.manager_process.is_alive(), "the manager exits once its worker drained")
        self.assertEqual(self.manager_process.exitcode, 0)
        self.assertLess(time.time() - stopped_at, _DRAIN_TIMEOUT_SECONDS, "the drain ended with the task")
        self.assertIsInstance(processor_pid, int)
