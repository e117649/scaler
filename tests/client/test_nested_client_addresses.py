"""Which object storage address a client opened inside a worker uses.

It takes its worker's address. The scheduler advertises a different one for clients outside the cluster.
"""

import unittest
from typing import Optional

from scaler.client.client import Client
from scaler.config.types.address import AddressConfig
from scaler.worker.agent.processor.processor import _current_processor


def resolve(address: Optional[str]) -> Optional[str]:
    """`Client.__resolve_object_storage_address`, whose name Python mangles."""
    return Client._Client__resolve_object_storage_address(address)  # type: ignore[attr-defined]


class _FakeProcessor:
    """Only what the address resolution reads."""

    def __init__(self, scheduler: str, storage: str) -> None:
        self._scheduler = AddressConfig.from_string(scheduler)
        self._storage = AddressConfig.from_string(storage)

    def scheduler_address(self) -> AddressConfig:
        return self._scheduler

    def object_storage_address(self) -> AddressConfig:
        return self._storage


class TestNestedClientAddresses(unittest.TestCase):
    def setUp(self) -> None:
        processor = _FakeProcessor("tcp://scheduler.inside:6378", "tcp://storage.inside:6379")
        self._token = _current_processor.set(processor)  # type: ignore[arg-type]
        self.addCleanup(_current_processor.reset, self._token)

    def test_a_client_inside_a_worker_takes_its_worker_addresses(self) -> None:
        self.assertEqual(resolve(None), "tcp://storage.inside:6379")

    def test_a_given_address_still_wins(self) -> None:
        self.assertEqual(resolve("tcp://elsewhere:1234"), "tcp://elsewhere:1234")


class TestClientOutsideAWorker(unittest.TestCase):
    def test_nothing_is_resolved_so_the_scheduler_is_asked(self) -> None:
        """Outside a worker there is nothing to inherit, and None means the advertised address."""
        self.assertIsNone(resolve(None))


if __name__ == "__main__":
    unittest.main()
