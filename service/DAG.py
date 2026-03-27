"""
DAG (Directed Acyclic Graph) implementation for Video Transcoding Service
"""
import logging
from datetime import datetime
from typing import Dict, List, Any, Callable
from models import TaskStatus

logger = logging.getLogger(__name__)


class Task:
    def __init__(self, name: str, function: Callable, dependencies: List[str] = None):
        self.name = name
        self.function = function
        self.dependencies = dependencies or []
        self.status = TaskStatus.PENDING
        self.result = None
        self.error = None

    def execute(self, context: Dict[str, Any]) -> bool:
        """Execute the task and return success status"""
        try:
            self.status = TaskStatus.RUNNING
            logger.info(f"task.started - {self.name}")
            self.result = self.function(context)
            self.status = TaskStatus.SUCCESS
            logger.info(f"task.completed - {self.name}: {self.result}")
            return True
        except Exception as e:
            self.status = TaskStatus.FAILED
            self.error = str(e)
            logger.error(f"task.failed - {self.name}: {self.error}")
            return False


class DAG:
    """Directed Acyclic Graph for task execution"""
    def __init__(self, name: str):
        self.name = name
        self.tasks: Dict[str, Task] = {}
        self.execution_log = []

    def add_task(self, task: Task):
        """Add a task to the DAG"""
        if task.name in self.tasks:
            raise ValueError(f"Task {task.name} already exists")
        self.tasks[task.name] = task

    def _get_ready_tasks(self) -> List[Task]:
        """Get tasks that are ready to execute (dependencies met)"""
        ready = []
        for task in self.tasks.values():
            if task.status != TaskStatus.PENDING:
                continue

            deps_met = all(
                self.tasks[dep].status == TaskStatus.SUCCESS
                for dep in task.dependencies
            )
            if deps_met:
                ready.append(task)
        return ready

    def execute(self, context: Dict[str, Any]) -> bool:
        """Execute all tasks in topological order"""
        logger.info(f"dag.started - {self.name}")
        print(f"\n{'='*60}")
        print(f"Executing DAG: {self.name}")
        print(f"{'='*60}\n")

        while True:
            ready_tasks = self._get_ready_tasks()

            if not ready_tasks:
                pending = [t for t in self.tasks.values() if t.status == TaskStatus.PENDING]
                if pending:
                    raise RuntimeError(f"DAG has circular dependency or failed tasks blocking: {[t.name for t in pending]}")
                break

            for task in ready_tasks:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Executing: {task.name}")
                success = task.execute(context)

                log_entry = {
                    'task': task.name,
                    'status': task.status.value,
                    'timestamp': datetime.now().isoformat(),
                    'result': task.result,
                    'error': task.error
                }
                self.execution_log.append(log_entry)

                if success:
                    print(f"  ✓ Success: {task.result}")
                else:
                    print(f"  ✗ Failed: {task.error}")
                    return False

        logger.info(f"dag.completed - {self.name}")
        print(f"\n{'='*60}")
        print(f"DAG Execution Complete: All tasks successful")
        print(f"{'='*60}\n")
        return True
