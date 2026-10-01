import unittest
from typing import Callable, Dict, Tuple

from scaler.protocol.capnp import Task
from scaler.scheduler.controllers.policies.simple_policy.allocation.capability_allocate_policy import (
    CapabilityAllocatePolicy,
)
from scaler.scheduler.controllers.policies.simple_policy.allocation.even_load_allocate_policy import (
    EvenLoadAllocatePolicy,
)
from scaler.scheduler.controllers.policies.simple_policy.allocation.mixins import TaskAllocatePolicy
from scaler.utility.identifiers import ClientID, TaskID, WorkerID
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name

QUEUE_SIZE = 10

_DRAINING = WorkerID(b"draining")
_SERVING = WorkerID(b"serving")

# each allocation policy, and the capabilities its workers and tasks carry
_POLICIES: Dict[str, Tuple[Callable[[], TaskAllocatePolicy], Dict[str, int]]] = {
    "even_load": (EvenLoadAllocatePolicy, {}),
    "capability": (CapabilityAllocatePolicy, {"capA": -1}),
}


def _task(index: int, capabilities: Dict[str, int]) -> Task:
    return Task(
        taskId=TaskID(f"{index:016d}".encode()),
        source=ClientID(b"client_id"),
        metadata=b"",
        funcObjectId=b"",
        functionArgs=[],
        capabilities=capabilities,
    )


class TestAllocatePolicyDrain(unittest.TestCase):
    """What a draining worker means to an allocation policy, whichever policy it is."""

    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_drain_returns_the_tasks_it_holds_once(self) -> None:
        for name, (make_policy, capabilities) in _POLICIES.items():
            with self.subTest(policy=name):
                policy = make_policy()
                policy.add_worker(_DRAINING, capabilities, QUEUE_SIZE)
                assigned = [policy.assign_task(_task(index, capabilities)) for index in range(3)]
                self.assertEqual(set(assigned), {_DRAINING})

                self.assertEqual(len(policy.drain_worker(_DRAINING)), 3)
                self.assertEqual(policy.drain_worker(_DRAINING), [])

    def test_a_draining_worker_gets_no_new_task(self) -> None:
        """Every new task goes to a serving worker, even one that holds more tasks."""
        for name, (make_policy, capabilities) in _POLICIES.items():
            with self.subTest(policy=name):
                policy = make_policy()
                policy.add_worker(_DRAINING, capabilities, QUEUE_SIZE)
                policy.add_worker(_SERVING, capabilities, QUEUE_SIZE)
                for index in range(4):
                    policy.assign_task(_task(index, capabilities))

                policy.drain_worker(_DRAINING)

                for index in range(4, 8):
                    self.assertEqual(policy.assign_task(_task(index, capabilities)), _SERVING)

    def test_a_drained_worker_does_not_block_assignment(self) -> None:
        """An idle draining worker must not make the policy refuse work that a serving worker can take."""
        for name, (make_policy, capabilities) in _POLICIES.items():
            with self.subTest(policy=name):
                policy = make_policy()
                policy.add_worker(_DRAINING, capabilities, QUEUE_SIZE)
                policy.drain_worker(_DRAINING)
                policy.add_worker(_SERVING, capabilities, QUEUE_SIZE)

                self.assertTrue(policy.has_available_worker(capabilities))
                self.assertEqual(policy.assign_task(_task(0, capabilities)), _SERVING)

    def test_only_draining_workers_means_no_available_worker(self) -> None:
        for name, (make_policy, capabilities) in _POLICIES.items():
            with self.subTest(policy=name):
                policy = make_policy()
                policy.add_worker(_DRAINING, capabilities, QUEUE_SIZE)
                policy.drain_worker(_DRAINING)

                self.assertFalse(policy.has_available_worker(capabilities))
                self.assertFalse(policy.assign_task(_task(0, capabilities)).is_valid())

    def test_balance_never_moves_work_to_a_draining_worker(self) -> None:
        """An idle draining worker is no receiver: the balancer would hand it tasks it will not run."""
        for name, (make_policy, capabilities) in _POLICIES.items():
            with self.subTest(policy=name):
                policy = make_policy()
                policy.add_worker(_SERVING, capabilities, QUEUE_SIZE)
                for index in range(6):
                    policy.assign_task(_task(index, capabilities))
                policy.add_worker(_DRAINING, capabilities, QUEUE_SIZE)
                policy.drain_worker(_DRAINING)

                self.assertEqual(policy.balance(), {})

    def test_remove_after_drain_releases_the_worker(self) -> None:
        for name, (make_policy, capabilities) in _POLICIES.items():
            with self.subTest(policy=name):
                policy = make_policy()
                policy.add_worker(_DRAINING, capabilities, QUEUE_SIZE)
                policy.assign_task(_task(0, capabilities))
                policy.drain_worker(_DRAINING)

                self.assertEqual(len(policy.remove_worker(_DRAINING)), 1)
                self.assertNotIn(_DRAINING, policy.get_worker_ids())
