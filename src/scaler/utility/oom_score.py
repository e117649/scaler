import logging
import os
import sys

logger = logging.getLogger(__name__)

# The kernel's range for /proc/<pid>/oom_score_adj: the OOM killer picks a process at the maximum first.
OOM_SCORE_ADJ_MAX = 1000

_OOM_SCORE_ADJ_PATH = "/proc/self/oom_score_adj"


def raise_oom_score_adj(target: int) -> None:
    """Make the OOM killer prefer this process, so it dies before the processes that supervise it.

    Only raises: an unprivileged process cannot lower its own value, and a supervisor must keep the value it has.
    Advisory, like process niceness: where it cannot be set, the process runs anyway.
    """
    if sys.platform != "linux":
        return

    try:
        with open(_OOM_SCORE_ADJ_PATH) as current_file:
            current = int(current_file.read())
        if target <= current:
            return
        with open(_OOM_SCORE_ADJ_PATH, "w") as target_file:
            target_file.write(str(min(target, OOM_SCORE_ADJ_MAX)))
    except (OSError, ValueError) as error:
        logger.warning(f"process {os.getpid()} could not raise its oom_score_adj to {target}: {error}")


def midway_to_max_oom_score_adj() -> int:
    """Halfway between this process's oom_score_adj and the maximum: above its supervisor, below its processors."""
    if sys.platform != "linux":
        return OOM_SCORE_ADJ_MAX

    try:
        with open(_OOM_SCORE_ADJ_PATH) as current_file:
            current = int(current_file.read())
    except (OSError, ValueError):
        return OOM_SCORE_ADJ_MAX
    return current + (OOM_SCORE_ADJ_MAX - current) // 2
