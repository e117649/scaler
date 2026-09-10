"""The Workers tab: what each worker is running, and what is queued behind it.

The scheduler reports a task running the moment it dispatches it, so a task is held by a worker well
before a processor picks it up. The processors name what is on a core; everything else the worker holds
is waiting there.
"""

import unittest
from typing import List

from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import (
    BinderStatus,
    ClientManagerStatus,
    ObjectManagerStatus,
    ProcessorStatus,
    Resource,
    ScalingManagerStatus,
    StateScheduler,
    StateTask,
    StateWorker,
    TaskManagerStatus,
    TaskState,
    WorkerManagerStatus,
    WorkerState,
    WorkerStatus,
)
from scaler.ui.app import WORKER_QUEUE_SAMPLE, BrowserView, WebGUIConfig, WebUIApp, _RenderCache

WORKER = b"Worker|one"


def make_app(retained: int = 1000) -> WebUIApp:
    config = WebGUIConfig(monitor_address=AddressConfig.from_string("tcp://127.0.0.1:6380"), task_log_max_size=retained)
    return WebUIApp(config)


def task_id(index: int) -> bytes:
    return index.to_bytes(16, "big")


def dispatch(app: WebUIApp, index: int, worker: bytes = WORKER, state: TaskState = TaskState.running) -> None:
    """A task state as the GUI receives it: capability reads need a deserialized struct."""
    app._process_task_state(
        StateTask.from_bytes(
            StateTask(
                taskId=task_id(index), functionName=b"work", state=state, worker=worker, capabilities=[], metadata=b""
            ).to_bytes()
        )
    )


def report(app: WebUIApp, running: List[int], queued: int = 0, worker: bytes = WORKER) -> None:
    """One status frame in which `worker` has a processor per running task."""
    processors = [
        ProcessorStatus(
            pid=100 + index,
            initialized=True,
            hasTask=True,
            suspended=False,
            resource=Resource(cpu=100, rss=1_000_000),
            currentTaskId=task_id(index),
            taskAgeSeconds=3,
        )
        for index in running
    ]
    status = StateScheduler(
        binder=BinderStatus(received=[], sent=[]),
        scheduler=Resource(cpu=0, rss=0),
        rssFree=0,
        clientManager=ClientManagerStatus(clients=[]),
        objectManager=ObjectManagerStatus(numberOfObjects=0),
        taskManager=TaskManagerStatus(stateToCount=[]),
        workerManager=WorkerManagerStatus(
            workers=[
                WorkerStatus(
                    workerId=worker,
                    agent=Resource(cpu=10, rss=1_000_000),
                    rssFree=8_000_000,
                    memLimit=16_000_000,
                    free=10,
                    sent=len(running) + queued,
                    queued=queued,
                    suspended=0,
                    lagUS=500,
                    lastS=1,
                    itl=" ",
                    processorStatuses=processors,
                    hostname="box-1",
                    netSentBytes=0,
                    netRecvBytes=0,
                )
            ]
        ),
        scalingManager=ScalingManagerStatus(managedWorkers=[], workerManagerDetails=[]),
    )
    app._process_scheduler(StateScheduler.from_bytes(status.to_bytes()))


def worker_card(app: WebUIApp) -> dict:
    section = app._worker_details_section(BrowserView(), _RenderCache())
    return section["worker_details"][0]["workers"][0]


class TestWorkerQueue(unittest.TestCase):
    def test_a_worker_separates_what_it_runs_from_what_is_waiting(self) -> None:
        app = make_app()
        for index in range(4):
            dispatch(app, index)
        report(app, running=[0], queued=3)

        card = worker_card(app)
        self.assertEqual(card["running"], 1)
        self.assertEqual(card["queue_depth"], 3, "what the worker itself reports queued")
        self.assertEqual(card["queue_named"], 3)
        self.assertEqual([entry["task_id"] for entry in card["queue"]], [task_id(i).hex() for i in (1, 2, 3)])
        self.assertEqual(card["queue"][0]["function"], "work")

    def test_the_running_task_carries_the_function_it_is_running(self) -> None:
        app = make_app()
        dispatch(app, 0)
        report(app, running=[0])

        processor = worker_card(app)["processors"][0]
        self.assertEqual(processor["function"], "work")
        self.assertEqual(processor["task_id"], task_id(0).hex(), "the whole id, so the card links to its trail")

    def test_a_finished_task_leaves_the_queue(self) -> None:
        app = make_app()
        for index in range(3):
            dispatch(app, index)
        dispatch(app, 1, state=TaskState.success)
        report(app, running=[0], queued=1)

        self.assertEqual([entry["task_id"] for entry in worker_card(app)["queue"]], [task_id(2).hex()])

    def test_a_rebalanced_task_is_queued_on_its_new_worker_alone(self) -> None:
        app = make_app()
        dispatch(app, 0, worker=b"Worker|two")
        dispatch(app, 0, worker=WORKER)
        report(app, running=[], queued=1)

        self.assertEqual([entry["task_id"] for entry in worker_card(app)["queue"]], [task_id(0).hex()])
        self.assertNotIn("Worker|two", app._worker_tasks)

    def test_a_long_queue_is_sampled_and_counted(self) -> None:
        app = make_app()
        for index in range(WORKER_QUEUE_SAMPLE + 10):
            dispatch(app, index)
        report(app, running=[], queued=WORKER_QUEUE_SAMPLE + 10)

        card = worker_card(app)
        self.assertEqual(len(card["queue"]), WORKER_QUEUE_SAMPLE)
        self.assertEqual(card["queue_named"], WORKER_QUEUE_SAMPLE + 10)

    def test_a_departed_worker_holds_nothing(self) -> None:
        app = make_app()
        dispatch(app, 0)
        report(app, running=[0])
        app._process_worker_state(
            StateWorker.from_bytes(
                StateWorker(workerId=WORKER, state=WorkerState.disconnected, capabilities=[]).to_bytes()
            )
        )

        self.assertEqual(app._worker_tasks, {})
        self.assertEqual(app._task_worker, {})

    def test_a_task_dropped_from_the_log_is_dropped_from_its_worker(self) -> None:
        """Retention bounds the log, and the tasks each worker holds are bounded with it."""
        app = make_app(retained=4)
        for index in range(6):
            dispatch(app, index)
        report(app, running=[], queued=6)

        self.assertEqual(len(app._task_worker), 4)
        self.assertEqual(worker_card(app)["queue_named"], 4)


if __name__ == "__main__":
    unittest.main()
