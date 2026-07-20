import asyncio
import unittest
from unittest.mock import MagicMock

from scaler.io.ymq import ConnectorSocketClosedByRemoteEndError, ErrorCode
from scaler.protocol.capnp import TaskState, TaskTransition
from scaler.scheduler.controllers.task_controller import VanillaTaskController
from scaler.utility.identifiers import TaskID


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class TestTaskControllerRoutingResilience(unittest.TestCase):
    """A send to a departed peer must never propagate out of __routing.

    Worker sends are rerouted by __send_to_worker, but a result/cancel-confirm bound for a client that
    has gone raises the same ConnectorSocketClosedByRemoteEndError. Raised from a timer loop (the
    balancer or the worker-cleanup loop) rather than the binder receive loop, re-raising it would
    propagate through asyncio.gather and terminate the whole scheduler.
    """

    @staticmethod
    def _controller() -> VanillaTaskController:
        controller = VanillaTaskController(config_controller=MagicMock())
        return controller

    def _drive_running_handler_raising(self, controller: VanillaTaskController, task_id: TaskID, error: Exception):
        controller._task_state_manager.add_state_machine(task_id)  # starts inactive

        async def handler(*_args, **_kwargs):
            raise error

        controller._state_functions[TaskState.running] = handler
        # inactive --hasCapacity--> running, then the (patched) running handler raises.
        routing = controller._VanillaTaskController__routing  # type: ignore[attr-defined]
        _run(routing(task_id, TaskTransition.hasCapacity, worker_id=None))

    def test_departed_peer_socket_closed_is_swallowed(self):
        controller = self._controller()
        # Must not raise: a departed peer is routine, not scheduler-fatal.
        self._drive_running_handler_raising(
            controller,
            TaskID(b"departed-peer-task"),
            ConnectorSocketClosedByRemoteEndError(ErrorCode.ConnectorSocketClosedByRemoteEnd, "client gone"),
        )

    def test_other_errors_still_propagate(self):
        controller = self._controller()
        # A genuine bug must still surface (the backstop is narrow, only for the departed-peer error).
        with self.assertRaises(ValueError):
            self._drive_running_handler_raising(controller, TaskID(b"real-bug-task"), ValueError("real bug"))


if __name__ == "__main__":
    unittest.main()
