import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from scaler.protocol.capnp import Task, TaskCancel
from scaler.utility.identifiers import ClientID, TaskID
from scaler.utility.metadata.task_flags import TaskFlags
from scaler.worker.agent.task_manager import VanillaTaskManager

# long enough for the task manager to start a queued task if it is going to
ROUTINE_WAIT_SECONDS = 0.2


def _task(index: int, priority: int = 0) -> Task:
    return Task(
        taskId=TaskID(f"{index:016d}".encode()),
        source=ClientID(b"client_id"),
        metadata=TaskFlags(priority=priority).serialize(),
        funcObjectId=b"",
        functionArgs=[],
        capabilities={},
    )


class TestVanillaTaskManagerDrain(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.task_manager = VanillaTaskManager(task_timeout_seconds=0)
        self.processor_manager = MagicMock()
        self.processor_manager.wait_until_can_accept_task = AsyncMock()
        self.processor_manager.on_task = AsyncMock()
        self.processor_manager.on_resume_task = AsyncMock()
        self.processor_manager.on_suspend_task = AsyncMock()
        self.processor_manager.current_task.return_value = None
        self.task_manager.register(connector=AsyncMock(), processor_manager=self.processor_manager)

    async def test_a_draining_worker_starts_no_queued_task(self) -> None:
        await self.task_manager.on_task_new(_task(0))
        self.task_manager.drain()

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.task_manager.routine(), ROUTINE_WAIT_SECONDS)

        self.processor_manager.on_task.assert_not_awaited()
        self.assertEqual(self.task_manager.get_queued_size(), 1, "the scheduler takes the queued task back")

    async def test_a_draining_worker_resumes_its_suspended_task(self) -> None:
        """A suspended task already holds a processor here: draining finishes it rather than abandoning it."""
        parent = _task(0)
        await self.task_manager.on_task_new(parent)
        await self.task_manager.routine()  # the parent starts
        self.processor_manager.current_task.return_value = parent

        nested = _task(1, priority=1)
        await self.task_manager.on_task_new(nested)  # a higher priority task suspends the parent
        self.task_manager.drain()
        await self.task_manager.on_cancel_task(
            TaskCancel(taskId=nested.taskId, flags=TaskCancel.TaskCancelFlags(force=False))
        )

        await asyncio.wait_for(self.task_manager.routine(), ROUTINE_WAIT_SECONDS)
        self.processor_manager.on_resume_task.assert_awaited_once_with(parent.taskId)

    async def test_draining_counts_the_tasks_that_hold_a_processor(self) -> None:
        await self.task_manager.on_task_new(_task(0))
        await self.task_manager.routine()
        self.task_manager.drain()

        self.assertTrue(self.task_manager.is_draining())
        self.assertEqual(self.task_manager.get_processing_size(), 1)
