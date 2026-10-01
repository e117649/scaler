from __future__ import annotations

import asyncio
import dataclasses
import logging
import multiprocessing
import multiprocessing.connection
import os
import pathlib
import signal
import sys
import time
import uuid
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.synchronize import Event as EventType
from typing import TYPE_CHECKING, Callable, List, Optional

from scaler.config.defaults import (
    DEFAULT_WORKER_RESTART_BACKOFF_SECONDS,
    MAX_WORKER_RESTART_BACKOFF_DOUBLINGS,
    WORKER_STABLE_SECONDS,
    WORKER_STOP_TIMEOUT_SECONDS,
    WORKER_SUPERVISION_INTERVAL_SECONDS,
)
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.utility.exitcode import WORKER_EXIT_CODE_SCHEDULER_UNREACHABLE, describe_exitcode
from scaler.worker.worker import Worker
from scaler.worker_manager.capacity_coordinator import CapacityCoordinator
from scaler.worker_manager.desired_concurrency import extract_desired_count
from scaler.worker_manager.mixins import DeclarativeWorkerProvisioner
from scaler.worker_manager.runner import WorkerManagerRunner

if TYPE_CHECKING:
    from scaler.protocol.capnp import WorkerManagerCommand

logger = logging.getLogger(__name__)

_SPAWN_CONTEXT = multiprocessing.get_context("spawn")


@dataclasses.dataclass
class _SupervisedWorker:
    worker: Worker
    drain_request: EventType
    processing_tasks: Synchronized
    started_at: float  # time.monotonic()
    drain_deadline: Optional[float] = None  # time.monotonic(), set once the worker is told to drain
    stop_deadline: Optional[float] = None  # time.monotonic(), set once the worker is told to stop


class NativeWorkerSupervisor:
    """Keeps `target` workers running on this machine.

    A worker that dies on its own is replaced, after a backoff that doubles with each consecutive death. A worker that
    exits 0 was asked to stop by the scheduler, and gives up its place. Workers above the target are drained, the
    idlest first: each finishes the tasks it runs, then exits. A drain that outlives its deadline ends in a stop.
    """

    def __init__(
        self,
        create_worker: Callable[[EventType, Synchronized], Worker],
        drain_timeout_seconds: float,
        busy_file: Optional[str] = None,
    ) -> None:
        self._create_worker = create_worker
        self._drain_timeout_seconds = drain_timeout_seconds
        self._busy_file = busy_file

        self._target = 0
        self._workers: List[_SupervisedWorker] = []
        self._shutting_down = False

        self._consecutive_deaths = 0
        self._start_not_before = 0.0  # time.monotonic()

    def set_target(self, count: int) -> None:
        self._target = count

    def active_count(self) -> int:
        """Workers that run and are not leaving."""
        return len(self.__serving())

    def sentinels(self) -> List[int]:
        return [supervised.worker.sentinel for supervised in self._workers]

    def is_done(self) -> bool:
        return (self._shutting_down or self._target == 0) and not self._workers

    def drain_all(self) -> None:
        """Drain every worker and start no new one."""
        self._shutting_down = True
        now = time.monotonic()
        for supervised in self.__serving():
            self.__drain(supervised, now)

    def stop_all(self) -> None:
        """Stop every worker at once and start no new one."""
        self._shutting_down = True
        now = time.monotonic()
        for supervised in self._workers:
            if supervised.stop_deadline is None:
                self.__stop(supervised, now)

    def routine(self) -> None:
        now = time.monotonic()
        self.__reap(now)
        self.__enforce_deadlines(now)
        if not self._shutting_down:
            self.__reconcile(now)
        self.__mark_busy()

    def __mark_busy(self) -> None:
        if self._busy_file is None:
            return

        # A draining worker's task counts: the pod is not idle until it finishes.
        busy = any(supervised.processing_tasks.value > 0 for supervised in self._workers)
        if busy:
            pathlib.Path(self._busy_file).touch()
        else:
            pathlib.Path(self._busy_file).unlink(missing_ok=True)

    def __serving(self) -> List[_SupervisedWorker]:
        return [supervised for supervised in self._workers if supervised.drain_deadline is None]

    def __reap(self, now: float) -> None:
        for supervised in [supervised for supervised in self._workers if not supervised.worker.is_alive()]:
            self._workers.remove(supervised)
            supervised.worker.join()
            exitcode = supervised.worker.exitcode
            identity = supervised.worker.identity

            if supervised.drain_deadline is not None or supervised.stop_deadline is not None:
                logger.info(f"native worker {identity!r} stopped (exitcode={describe_exitcode(exitcode)})")
                continue

            if exitcode == 0:
                # Only a stop that someone asked for ends in 0, so the worker gives up its place.
                self._target = max(0, self._target - 1)
                logger.info(f"native worker {identity!r} shut down cleanly")
                continue

            if exitcode == WORKER_EXIT_CODE_SCHEDULER_UNREACHABLE:
                # A replacement would not reach the scheduler either.
                self._target = max(0, self._target - 1)
                logger.warning(f"native worker {identity!r} gave up on the scheduler, not replacing it")
                continue

            self._consecutive_deaths += 1
            doublings = min(self._consecutive_deaths - 1, MAX_WORKER_RESTART_BACKOFF_DOUBLINGS)
            backoff_seconds = DEFAULT_WORKER_RESTART_BACKOFF_SECONDS * 2**doublings
            self._start_not_before = now + backoff_seconds
            logger.warning(
                f"native worker {identity!r} exited unexpectedly (exitcode={describe_exitcode(exitcode)}), "
                f"replacing it in {backoff_seconds}s"
            )

        if any(now - supervised.started_at > WORKER_STABLE_SECONDS for supervised in self.__serving()):
            self._consecutive_deaths = 0

    def __enforce_deadlines(self, now: float) -> None:
        for supervised in self._workers:
            if supervised.stop_deadline is not None:
                if now > supervised.stop_deadline and supervised.worker.is_alive():
                    logger.warning(f"native worker {supervised.worker.identity!r} did not stop, killing it")
                    supervised.worker.kill()
                continue

            if supervised.drain_deadline is not None and now > supervised.drain_deadline:
                logger.warning(
                    f"native worker {supervised.worker.identity!r} did not drain in {self._drain_timeout_seconds}s, "
                    f"stopping it"
                )
                self.__stop(supervised, now)

    def __reconcile(self, now: float) -> None:
        serving = self.__serving()
        if len(serving) > self._target:
            idlest_first = sorted(
                serving, key=lambda supervised: (supervised.processing_tasks.value, -supervised.started_at)
            )
            for supervised in idlest_first[: len(serving) - self._target]:
                self.__drain(supervised, now)
            return

        if now < self._start_not_before:
            return

        for _ in range(self._target - len(serving)):
            self.__start(now)

    def __start(self, now: float) -> None:
        drain_request = _SPAWN_CONTEXT.Event()
        processing_tasks = _SPAWN_CONTEXT.Value("i", 0)
        worker = self._create_worker(drain_request, processing_tasks)
        worker.start()
        self._workers.append(_SupervisedWorker(worker, drain_request, processing_tasks, started_at=now))
        logger.info(f"started native worker {worker.identity!r}")

    def __drain(self, supervised: _SupervisedWorker, now: float) -> None:
        logger.info(
            f"draining native worker {supervised.worker.identity!r} "
            f"({supervised.processing_tasks.value} task(s) to finish, {self._drain_timeout_seconds}s at most)"
        )
        supervised.drain_request.set()
        supervised.drain_deadline = now + self._drain_timeout_seconds

    @staticmethod
    def __stop(supervised: _SupervisedWorker, now: float) -> None:
        supervised.stop_deadline = now + WORKER_STOP_TIMEOUT_SECONDS
        if sys.platform == "win32":
            # TerminateProcess is forceful: the worker's teardown does not run, so the scheduler times it out.
            supervised.worker.terminate()
        elif supervised.worker.is_alive():
            os.kill(supervised.worker.pid, signal.SIGTERM)


class NativeWorkerProvisioner(DeclarativeWorkerProvisioner):
    def __init__(self, config: NativeWorkerManagerConfig) -> None:
        self._worker_scheduler_address = config.worker_manager_config.effective_worker_scheduler_address
        self._object_storage_address = config.worker_manager_config.object_storage_address
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._worker_manager_id = config.worker_manager_config.worker_manager_id.encode()
        self._io_threads = config.worker_config.io_threads
        self._task_queue_size = config.worker_config.per_worker_task_queue_size
        self._max_task_concurrency = config.worker_manager_config.max_task_concurrency
        self._heartbeat_interval_seconds = config.worker_config.heartbeat_interval_seconds
        self._task_timeout_seconds = config.worker_config.task_timeout_seconds
        self._death_timeout_seconds = config.worker_config.death_timeout_seconds
        self._garbage_collect_interval_seconds = config.worker_config.garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = config.worker_config.trim_memory_threshold_bytes
        self._hard_processor_suspend = config.worker_config.hard_processor_suspend
        self._event_loop = config.worker_config.event_loop
        self._preload = config.worker_config.preload
        self._logging_paths = config.logging_config.paths
        self._logging_level = config.logging_config.level
        self._security_config = config.security

        if config.worker_type is not None:
            self._worker_prefix = config.worker_type
        elif config.mode == NativeWorkerManagerMode.FIXED:
            self._worker_prefix = "FIX"
        elif config.mode == NativeWorkerManagerMode.DYNAMIC:
            self._worker_prefix = "NAT"
        else:
            raise ValueError(f"worker_type is not set and mode is unrecognised: {config.mode!r}")

        self._supervisor = NativeWorkerSupervisor(
            self._create_worker, config.worker_manager_config.drain_timeout_seconds, config.busy_file
        )
        self._capacity_coordinator = CapacityCoordinator(
            start_units=self.start_units,
            stop_units=self.stop_units,
            active_unit_count=self.active_unit_count,
            max_unit_count=self._max_task_concurrency,
            scale_down_cooldown_seconds=config.worker_manager_config.scale_down_cooldown_seconds,
        )

    def _create_worker(self, drain_request: EventType, processing_tasks: Synchronized) -> Worker:
        return Worker(
            name=f"{self._worker_prefix}|{uuid.uuid4().hex}",
            address=self._worker_scheduler_address,
            object_storage_address=self._object_storage_address,
            preload=self._preload,
            capabilities=self._capabilities,
            io_threads=self._io_threads,
            task_queue_size=self._task_queue_size,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            task_timeout_seconds=self._task_timeout_seconds,
            death_timeout_seconds=self._death_timeout_seconds,
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
            hard_processor_suspend=self._hard_processor_suspend,
            event_loop=self._event_loop,
            logging_paths=self._logging_paths,
            logging_level=self._logging_level,
            worker_manager_id=self._worker_manager_id,
            security_config=self._security_config,
            drain_request=drain_request,
            processing_tasks=processing_tasks,
        )

    def run_fixed(self) -> None:
        """Keep max_task_concurrency workers running until told to stop.

        SIGTERM drains: each worker finishes the tasks it runs, and one still running after drain_timeout_seconds is
        stopped. SIGINT, or a second signal, stops every worker at once.
        """
        self._supervisor.set_target(self._max_task_concurrency)

        received_signals: List[int] = []
        signal.signal(signal.SIGTERM, lambda signum, frame: received_signals.append(signum))
        signal.signal(signal.SIGINT, lambda signum, frame: received_signals.append(signum))

        handled_signals = 0
        while not self._supervisor.is_done():
            for signum in received_signals[handled_signals:]:
                handled_signals += 1
                if signum == signal.SIGTERM and handled_signals == 1:
                    logger.info("NativeWorkerProvisioner (FIXED): received SIGTERM, draining workers")
                    self._supervisor.drain_all()
                else:
                    logger.info(f"NativeWorkerProvisioner (FIXED): received signal {signum}, stopping workers")
                    self._supervisor.stop_all()

            self._supervisor.routine()

            sentinels = self._supervisor.sentinels()
            if sentinels:
                multiprocessing.connection.wait(sentinels, timeout=WORKER_SUPERVISION_INTERVAL_SECONDS)
            else:
                time.sleep(WORKER_SUPERVISION_INTERVAL_SECONDS)

    async def set_desired_task_concurrency(
        self, requests: List[WorkerManagerCommand.DesiredTaskConcurrencyRequest]
    ) -> None:
        task_concurrency = extract_desired_count(requests, self._capabilities)
        self._supervisor.routine()
        await self._capacity_coordinator.set_desired_unit_count(task_concurrency)

    def active_unit_count(self) -> int:
        return self._supervisor.active_count()

    async def start_units(self, count: int) -> None:
        self._supervisor.set_target(self._supervisor.active_count() + count)
        self._supervisor.routine()

    async def stop_units(self, count: int) -> None:
        self._supervisor.set_target(max(0, self._supervisor.active_count() - count))
        self._supervisor.routine()

    async def terminate(self) -> None:
        self._capacity_coordinator.cancel()
        self._supervisor.stop_all()
        while not self._supervisor.is_done():
            self._supervisor.routine()
            await asyncio.sleep(WORKER_SUPERVISION_INTERVAL_SECONDS)


class NativeWorkerManager:
    def __init__(self, config: NativeWorkerManagerConfig) -> None:
        self._config = config

    @property
    def config(self) -> NativeWorkerManagerConfig:
        return self._config

    def run(self) -> None:
        provisioner = NativeWorkerProvisioner(self._config)

        if self._config.mode == NativeWorkerManagerMode.FIXED:
            provisioner.run_fixed()
            return

        runner = WorkerManagerRunner(
            address=self._config.worker_manager_config.scheduler_address,
            name="worker_manager_native",
            heartbeat_interval_seconds=self._config.worker_config.heartbeat_interval_seconds,
            capabilities=self._config.worker_config.per_worker_capabilities.capabilities,
            max_provisioner_units=self._config.worker_manager_config.max_task_concurrency,
            worker_manager_id=self._config.worker_manager_config.worker_manager_id.encode(),
            worker_provisioner=provisioner,
            io_threads=self._config.worker_config.io_threads,
            security_config=self._config.security,
        )
        runner.run()
