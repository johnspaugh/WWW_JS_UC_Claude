import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from enum import Enum
from typing import Any, Dict, List, Tuple

from ffmpeg_runner import encode as ffmpeg_encode, FFmpegError


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED  = "failed"


# ---------------------------------------------------------------------------
# Task type registry
# Maps the 'type' field in a workflow task dict to a callable.
# ---------------------------------------------------------------------------

def _run_encode(task: Dict[str, Any]) -> Dict[str, Any]:
    return ffmpeg_encode(task["source_path"], task["profile"], task["output_dir"])


TASK_HANDLERS = {
    "encode": _run_encode,
}


# ---------------------------------------------------------------------------
# DAGExecutor
# ---------------------------------------------------------------------------

class DAGExecutor:
    """
    Loads a DAG from a workflow dict, resolves each task to a handler,
    and executes them in parallel using a worker pool.

    Workflow dict format:
        {
            "id": "dag_uuid",
            "tasks": [
                {
                    "id":           str,        # unique within the DAG
                    "name":         str,        # human-readable label
                    "type":         str,        # key in TASK_HANDLERS
                    "dependencies": [str, ...], # ids of tasks that must succeed first
                    # ... type-specific fields (e.g. source_path, profile, output_dir)
                },
                ...
            ]
        }
    """

    def __init__(self, max_workers: int = 4, failure_threshold: int = 1):
        self._max_workers = max_workers
        self._failure_threshold = failure_threshold

    def execute(self, dag: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], Dict[str, Exception]]:
        """
        Load and execute a DAG.

        Returns (success, results_by_task_id, errors_by_task_id).
        """
        tasks = {t["id"]: t for t in dag["tasks"]}
        status: Dict[str, TaskStatus] = {tid: TaskStatus.PENDING for tid in tasks}
        results: Dict[str, Any] = {}
        errors: Dict[str, Exception] = {}
        lock = threading.Lock()
        failure_count = 0

        print(f"\n[DAGExecutor] DAG '{dag['id']}' — "
              f"{len(tasks)} tasks, {self._max_workers} workers")

        def root_tasks() -> List[str]:
            return [tid for tid, t in tasks.items() if not t["dependencies"]]

        def newly_unblocked() -> List[str]:
            with lock:
                return [
                    tid for tid, t in tasks.items()
                    if status[tid] == TaskStatus.PENDING
                    and all(status[dep] == TaskStatus.SUCCESS for dep in t["dependencies"])
                ]

        def submit(pool, futures, tid):
            handler = TASK_HANDLERS.get(tasks[tid]["type"])
            if handler is None:
                raise ValueError(f"Unknown task type: '{tasks[tid]['type']}'")
            with lock:
                status[tid] = TaskStatus.RUNNING
            print(f"[DAGExecutor] Submitting: {tasks[tid]['name']} ({tid})")
            futures[pool.submit(handler, tasks[tid])] = tid

        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures: Dict[Any, str] = {}

            for tid in root_tasks():
                submit(pool, futures, tid)

            if not futures:
                print("[DAGExecutor] No runnable tasks — check dependencies or task list")
                return False, results, errors

            while futures:
                done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)

                for future in done:
                    tid = futures.pop(future)
                    exc = future.exception()

                    if exc is None:
                        with lock:
                            status[tid] = TaskStatus.SUCCESS
                            results[tid] = future.result()
                        print(f"[DAGExecutor] SUCCESS: {tasks[tid]['name']}")

                        for new_tid in newly_unblocked():
                            submit(pool, futures, new_tid)
                    else:
                        with lock:
                            status[tid] = TaskStatus.FAILED
                            errors[tid] = exc
                            failure_count += 1
                        print(f"[DAGExecutor] FAILED:  {tasks[tid]['name']} — {exc}")

                        if failure_count >= self._failure_threshold:
                            print("[DAGExecutor] Failure threshold reached — aborting")
                            return False, results, errors

        all_ok = all(s == TaskStatus.SUCCESS for s in status.values())
        if all_ok:
            print(f"[DAGExecutor] All tasks completed successfully")
        else:
            blocked = [tid for tid, s in status.items() if s == TaskStatus.PENDING]
            if blocked:
                print(f"[DAGExecutor] Never ran (blocked): {blocked}")

        return all_ok, results, errors
