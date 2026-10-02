import os
import sys
import tempfile
import unittest
from unittest.mock import patch

from scaler.utility import oom_score
from scaler.utility.oom_score import OOM_SCORE_ADJ_MAX, midway_to_max_oom_score_adj, raise_oom_score_adj


@unittest.skipUnless(sys.platform == "linux", "oom_score_adj is a Linux /proc file")
class TestOOMScore(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = os.path.join(directory.name, "oom_score_adj")
        path_patch = patch.object(oom_score, "_OOM_SCORE_ADJ_PATH", self.path)
        path_patch.start()
        self.addCleanup(path_patch.stop)

    def _write(self, value: int) -> None:
        with open(self.path, "w") as score_file:
            score_file.write(f"{value}\n")

    def _read(self) -> int:
        with open(self.path) as score_file:
            return int(score_file.read())

    def test_raises_to_the_target(self) -> None:
        self._write(992)
        raise_oom_score_adj(OOM_SCORE_ADJ_MAX)
        self.assertEqual(self._read(), OOM_SCORE_ADJ_MAX)

    def test_never_lowers(self) -> None:
        """An unprivileged process cannot lower its value, and a supervisor keeps the one it has."""
        self._write(500)
        raise_oom_score_adj(100)
        self.assertEqual(self._read(), 500)

    def test_midway_sits_between_the_supervisor_and_the_maximum(self) -> None:
        self._write(992)
        self.assertEqual(midway_to_max_oom_score_adj(), 996)
        self._write(-997)
        self.assertEqual(midway_to_max_oom_score_adj(), 1)

    def test_a_missing_file_is_only_a_warning(self) -> None:
        with self.assertLogs(oom_score.logger, level="WARNING"):
            raise_oom_score_adj(OOM_SCORE_ADJ_MAX)
