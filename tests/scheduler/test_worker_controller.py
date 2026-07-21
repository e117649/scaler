import asyncio
import time
import unittest
from unittest.mock import MagicMock

from scaler.io.ymq import ConnectorSocketClosedByRemoteEndError, ErrorCode
from scaler.protocol.capnp import Task
from scaler.scheduler.controllers.task_controller import VanillaTaskController
from scaler.scheduler.controllers.vanilla_policy_controller import VanillaPolicyController
from scaler.scheduler.controllers.worker_controller import VanillaWorkerController
from scaler.utility.identifiers import ClientID, TaskID, WorkerID


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class _DeadableBinder:
    """An async binder whose send() fails as a departed peer for any worker marked dead."""

    def __init__(self):
        self.dead = set()

    async def send(self, to, message):
        if WorkerID(bytes(to)) in self.dead:
            raise ConnectorSocketClosedByRemoteEndError(
                ErrorCode.ConnectorSocketClosedByRemoteEnd, "worker socket closed by remote end"
            )


class _NullMonitor:
    async def send(self, message):
        return None


class TestWorkerControllerMassEviction(unittest.TestCase):
    """Rerouting departed workers' tasks must not recurse once per dead worker.

    With one shared capability every task fits every worker, so a task shed from a dead worker is
    reassigned to another dead-but-still-registered worker; that worker's failed send reports it departed
    and re-enters the disconnect path. When a whole batch of pods drops at once this used to recurse one
    stack frame deeper per dead worker and blow Python's recursion limit, crashing the scheduler. The
    disconnect must drain the departed workers iteratively instead.
    """

    N_WORKERS = 300  # comfortably past the ~90-deep point where the old recursion hit the limit

    @staticmethod
    def _make_task(index: int) -> Task:
        return Task(
            taskId=TaskID(f"task-{index}".encode()),
            source=ClientID(b"client"),
            metadata=b"",
            funcObjectId=b"",
            functionArgs=[],
            capabilities={},
        )

    def test_mass_eviction_reroute_does_not_recurse(self):
        config = MagicMock()
        policy = VanillaPolicyController("simple", "allocate=capability; scaling=vanilla")
        worker_controller = VanillaWorkerController(config, policy)
        task_controller = VanillaTaskController(config)

        binder = _DeadableBinder()
        monitor = _NullMonitor()
        client_controller = MagicMock()
        client_controller.on_task_finish.return_value = None
        object_controller = MagicMock()
        object_controller.get_object_name.return_value = b""
        graph_controller = MagicMock()
        graph_controller.is_graph_subtask.return_value = False

        worker_controller.register(binder, monitor, task_controller)
        task_controller.register(
            binder, monitor, client_controller, object_controller, worker_controller, graph_controller
        )

        # Register N live workers (bypassing on_heartbeat; replicate the state it maintains).
        manager_id = b"pod-manager"
        for i in range(self.N_WORKERS):
            worker_id = WorkerID(f"worker-{i}".encode())
            policy.add_worker(worker_id, {"capA": -1}, 10)
            worker_controller._worker_alive_since[worker_id] = (time.time(), None)
            worker_controller._worker_to_manager[worker_id] = manager_id
            worker_controller._manager_to_workers.setdefault(manager_id, set()).add(worker_id)

        async def scenario():
            for i in range(self.N_WORKERS):  # one running task per worker, all sent while live
                await task_controller.on_task_new(self._make_task(i))
            for i in range(self.N_WORKERS):  # a batch of pods is evicted at once
                binder.dead.add(WorkerID(f"worker-{i}".encode()))
            # Must not raise RecursionError: the reroute cascade drains iteratively, not recursively.
            await worker_controller.on_worker_departed(WorkerID(b"worker-0"))

        _run(scenario())

        # Every dead worker was disconnected; none is left registered.
        self.assertEqual(len(policy.get_worker_ids()), 0)


if __name__ == "__main__":
    unittest.main()
