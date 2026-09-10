"""The workers table's columns, which three files have to agree on.

`WORKER_FIELDS` in app.js is the table's column order: the page builds the header from it, sends a header
click back by that name, and fills each cell from the row field of the same name. A column added to one
file and not the others silently writes into the wrong cell, so this pins all three together.
"""

import json
import re
import unittest

from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import (
    BinderStatus,
    ClientManagerStatus,
    ObjectManagerStatus,
    ProcessorStatus,
    Resource,
    ScalingManagerStatus,
    StateScheduler,
    TaskManagerStatus,
    WorkerManagerStatus,
    WorkerStatus,
)
from scaler.ui.app import STATIC_DIR, WORKER_SORT_FIELDS, WebGUIConfig, WebUIApp


def worker_fields() -> list:
    source = (STATIC_DIR / "app.js").read_text()
    listing = re.search(r"var WORKER_FIELDS = (\[.*?\]);", source, re.DOTALL)
    assert listing is not None, "app.js no longer declares WORKER_FIELDS"
    return json.loads(re.sub(r"\s+", " ", listing.group(1)))


def worker_headers() -> list:
    page = (STATIC_DIR / "index.html").read_text()
    table = page.split('id="workers-table"', 1)[1].split("</thead>", 1)[0]
    return re.findall(r"<th>(.*?)</th>", table)


def worker_row() -> dict:
    app = WebUIApp(WebGUIConfig(monitor_address=AddressConfig.from_string("tcp://127.0.0.1:6380")))
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
                    workerId=b"Worker|one",
                    agent=Resource(cpu=10, rss=1_000_000),
                    rssFree=8_000_000,
                    memLimit=16_000_000,
                    free=2,
                    sent=3,
                    queued=4,
                    suspended=0,
                    lagUS=500,
                    lastS=1,
                    itl=" ",
                    processorStatuses=[
                        ProcessorStatus(
                            pid=42,
                            initialized=True,
                            hasTask=True,
                            suspended=False,
                            resource=Resource(cpu=250, rss=2_000_000),
                            currentTaskId=b"\xab\xcd" * 16,
                            taskAgeSeconds=7,
                        )
                    ],
                    hostname="box-1",
                    netSentBytes=100,
                    netRecvBytes=200,
                )
            ]
        ),
        scalingManager=ScalingManagerStatus(managedWorkers=[], workerManagerDetails=[]),
    )
    app._process_scheduler(StateScheduler.from_bytes(status.to_bytes()))
    return app._workers_data["Worker|one"]


class TestWorkersTableColumns(unittest.TestCase):
    def test_every_column_has_a_header(self) -> None:
        self.assertEqual(len(worker_headers()), len(worker_fields()))

    def test_every_column_is_a_field_of_the_row_the_backend_builds(self) -> None:
        missing = [field for field in worker_fields() if field not in worker_row()]
        self.assertEqual(missing, [], f"app.js names columns the backend never sends: {missing}")

    def test_every_column_can_be_sorted_by(self) -> None:
        """A header click sends its field name, which the server drops unless it can sort by it."""
        missing = [field for field in worker_fields() if field not in WORKER_SORT_FIELDS]
        self.assertEqual(missing, [], f"columns the server will not sort by: {missing}")


class TestWorkerRow(unittest.TestCase):
    def test_a_busy_worker_reports_its_task_and_how_long_it_has_held_it(self) -> None:
        row = worker_row()
        self.assertEqual(row["host"], "box-1")
        self.assertEqual(row["task"], "abcdabcdabcd")
        self.assertEqual((row["task_age"], row["task_age_s"]), ("7s", 7))


if __name__ == "__main__":
    unittest.main()
