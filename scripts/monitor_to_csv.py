#!/usr/bin/env python3
"""Append one CSV row per scheduler status report: the time, the worker count, and the task counts.

    python scripts/monitor_to_csv.py tcp://127.0.0.1:6380 counts.csv

The scheduler sends a report every status_report_interval_seconds (default 1).
Rows reach the file every 10 seconds, and SIGINT or SIGTERM writes the remaining rows before exiting.
The tasks column counts unfinished tasks, and the columns for completed states count up from the scheduler's start.
The running column includes tasks queued on a worker.
"""

import collections
import csv
import dataclasses
import datetime
import signal
import sys
import threading
from typing import Deque, List, TextIO

from scaler.config.common.security import SecurityConfig
from scaler.config.config_class import ConfigClass
from scaler.config.types.address import AddressConfig
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.utility import generate_identity_from_name
from scaler.protocol.capnp import BaseMessage, StateScheduler, TaskState

WRITE_INTERVAL_SECONDS = 10

UNFINISHED_TASK_STATES = (TaskState.inactive, TaskState.running, TaskState.canceling, TaskState.balanceCanceling)
CSV_HEADER = ["timestamp", "workers", "tasks", *(state.name for state in TaskState)]


@dataclasses.dataclass
class MonitorToCSVConfig(ConfigClass):
    monitor_address: AddressConfig = dataclasses.field(
        metadata=dict(positional=True, help="scheduler monitor address to connect to")
    )
    csv_path: str = dataclasses.field(metadata=dict(positional=True, help="CSV file to append to"))
    security: SecurityConfig = dataclasses.field(default_factory=SecurityConfig)


def status_to_row(status: StateScheduler) -> List[object]:
    state_to_count = {TaskState(pair.state): pair.count for pair in status.taskManager.stateToCount}
    return [
        datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds"),
        len(status.workerManager.workers),
        sum(state_to_count[state] for state in UNFINISHED_TASK_STATES),
        *(state_to_count[state] for state in TaskState),
    ]


def write_rows(rows: Deque[List[object]], csv_file: TextIO) -> None:
    writer = csv.writer(csv_file)
    while rows:
        writer.writerow(rows.popleft())

    csv_file.flush()


def main() -> int:
    config = MonitorToCSVConfig.parse("Dump scheduler task and worker counts to a CSV file", "monitor_to_csv")

    # the subscriber thread appends and the main thread pops, so only the main thread touches the file
    rows: Deque[List[object]] = collections.deque()

    def on_message(message: BaseMessage) -> None:
        if isinstance(message, StateScheduler):
            rows.append(status_to_row(message))

    backend = get_network_backend_from_env()
    # no timeout: a stalled scheduler is a gap in the file rather than the end of it
    subscriber = backend.create_sync_subscriber(
        identity=generate_identity_from_name("monitor_to_csv"),
        address=config.monitor_address,
        callback=on_message,
        timeout=None,
        security_config=config.security,
    )
    subscriber.daemon = True

    stop_requested = threading.Event()

    def request_stop(*_args: object) -> None:
        stop_requested.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    with open(config.csv_path, "a", newline="") as csv_file:
        if csv_file.tell() == 0:
            csv.writer(csv_file).writerow(CSV_HEADER)

        subscriber.start()
        while subscriber.is_alive() and not stop_requested.wait(WRITE_INTERVAL_SECONDS):
            write_rows(rows, csv_file)

        write_rows(rows, csv_file)

    return 0 if stop_requested.is_set() else 1


if __name__ == "__main__":
    sys.exit(main())
