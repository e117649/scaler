import os
import signal
import sys
import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.config.defaults import DEFAULT_HEARTBEAT_INTERVAL_SECONDS
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from tests.utility.utility import logging_test_name

RESULT_TIMEOUT_SECONDS = 30


@unittest.skipIf(sys.platform == "win32", "sends SIGKILL to a processor")
class TestIdleProcessorDeath(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        self.address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self.cluster = SchedulerClusterCombo(address=self.address, n_workers=1, event_loop="builtin")
        self.addCleanup(self.cluster.shutdown)

    def test_a_worker_keeps_serving_after_its_idle_processor_is_killed(self) -> None:
        """An idle processor killed by the OS, as an OOM kill does, is replaced and the worker takes the next task."""

        with Client(self.address) as client:
            processor_pid = client.submit(os.getpid).result(timeout=RESULT_TIMEOUT_SECONDS)

            os.kill(processor_pid, signal.SIGKILL)  # type: ignore[attr-defined, unused-ignore]

            # the worker notices on its next heartbeat and starts a new processor
            time.sleep(3 * DEFAULT_HEARTBEAT_INTERVAL_SECONDS)

            self.assertNotEqual(client.submit(os.getpid).result(timeout=RESULT_TIMEOUT_SECONDS), processor_pid)


if __name__ == "__main__":
    unittest.main()
