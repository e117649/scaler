import os
import tempfile
import time
import unittest
from typing import Tuple

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from tests.utility.utility import logging_test_name

N_TASKS = 30
N_WORKERS = 3
assert N_TASKS >= N_WORKERS

TICK_SECONDS = 0.1
CHILD_START_TIMEOUT_SECONDS = 30
# well below the 60 second client timeout, which is what stops an orphan without the parent link
CHILD_STOP_TIMEOUT_SECONDS = 15
QUIET_SECONDS = 1


class TestNestedTask(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self.cluster = SchedulerClusterCombo(address=self.address, n_workers=N_WORKERS, event_loop="builtin")

    def tearDown(self) -> None:
        self.cluster.shutdown()

    def test_nested_task_arg_client(self) -> None:
        with Client(self.address) as client:
            result = client.submit(parent_task_arg_client, client).result()
            self.assertEqual(result, sum(nested_task(v) for v in range(0, N_TASKS)))

    def test_recursive_task(self) -> None:
        with Client(self.address) as client:
            result = client.submit(factorial, client, 10).result()
            self.assertEqual(result, 3_628_800)

    def test_multiple_recursive_task(self) -> None:
        with Client(self.address) as client:
            result = client.submit(fibonacci, client, 8).result()
            self.assertEqual(result, 21)

    def test_nested_task_auto_address(self) -> None:
        """Test nested task with automatic scheduler address detection."""
        with Client(self.address) as client:
            result = client.submit(nested_task_auto_address, 5).result()
            self.assertEqual(result, 25)

    def test_nested_task_explicit_address(self) -> None:
        """Test nested task with explicit scheduler address (honors user-provided address)."""
        with Client(self.address) as client:
            result = client.submit(nested_task_explicit_address, self.address, 7).result()
            self.assertEqual(result, 49)

    def test_a_nested_task_names_its_parent(self) -> None:
        with Client(self.address) as client:
            parent_task_id, childs_parent_task_id = client.submit(submit_a_child_reporting_its_parent).result()
            self.assertEqual(childs_parent_task_id, parent_task_id)
            self.assertEqual(client.submit(own_parent_task_id).result(), b"")

    def test_a_nested_graph_names_its_parent(self) -> None:
        with Client(self.address) as client:
            parent_task_id, childs_parent_task_id = client.submit(submit_a_graph_reporting_its_parent).result()
            self.assertEqual(childs_parent_task_id, parent_task_id)

    def test_canceling_a_parent_cancels_its_children(self) -> None:
        """The cancel kills the parent's processor and its nested client, so only the scheduler can stop the child."""
        with tempfile.TemporaryDirectory() as directory, Client(self.address) as client:
            ticks_path = os.path.join(directory, "ticks")
            parent = client.submit(wait_on_a_ticking_child, ticks_path)

            deadline = time.time() + CHILD_START_TIMEOUT_SECONDS
            while not os.path.exists(ticks_path):
                self.assertLess(time.time(), deadline, "the child never started")
                time.sleep(TICK_SECONDS)

            parent.cancel()

            deadline = time.time() + CHILD_STOP_TIMEOUT_SECONDS
            size = os.path.getsize(ticks_path)
            while True:
                time.sleep(QUIET_SECONDS)
                previous_size, size = size, os.path.getsize(ticks_path)
                if size == previous_size:
                    break
                self.assertLess(time.time(), deadline, "the child kept running after its parent was canceled")


def parent_task_arg_client(client: Client) -> int:
    return sum(client.map(nested_task, range(0, N_TASKS)))


def nested_task(value: int) -> int:
    return value**2


def factorial(client: Client, value: int) -> int:
    if value == 0:
        return 1
    else:
        return value * client.submit(factorial, client, value - 1).result()


def fibonacci(client: Client, n: int):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        a = client.submit(fibonacci, client, n - 1)
        b = client.submit(fibonacci, client, n - 2)
        return a.result() + b.result()


def nested_task_auto_address(value: int) -> int:
    """Test function that creates a nested client without providing an address."""
    # Client should automatically detect worker context and use worker's scheduler address
    client = Client()
    try:
        result = client.submit(nested_task, value).result()
        return result
    finally:
        client.disconnect()


def own_parent_task_id() -> bytes:
    from scaler.worker.agent.processor.processor import Processor

    return Processor.get_current_processor().current_task().parentTaskId


def submit_a_child_reporting_its_parent() -> Tuple[bytes, bytes]:
    from scaler.worker.agent.processor.processor import Processor

    own_task_id = Processor.get_current_processor().current_task().taskId
    with Client() as client:
        return own_task_id, client.submit(own_parent_task_id).result()


def submit_a_graph_reporting_its_parent() -> Tuple[bytes, bytes]:
    from scaler.worker.agent.processor.processor import Processor

    own_task_id = Processor.get_current_processor().current_task().taskId
    with Client() as client:
        return own_task_id, client.get({"child": (own_parent_task_id,)}, keys=["child"])["child"]


def wait_on_a_ticking_child(ticks_path: str) -> None:
    with Client() as client:
        client.submit(tick_forever, ticks_path).result()


def tick_forever(ticks_path: str) -> None:
    while True:
        with open(ticks_path, "a") as ticks:
            ticks.write("tick\n")
        time.sleep(TICK_SECONDS)


def nested_task_explicit_address(scheduler_address: str, value: int) -> int:
    """Test function that creates a nested client with an explicit address."""
    # Client should honor the explicitly provided address even though running in worker context
    client = Client(address=scheduler_address)
    try:
        result = client.submit(nested_task, value).result()
        return result
    finally:
        client.disconnect()
