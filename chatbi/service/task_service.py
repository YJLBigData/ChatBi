import os
import queue
import threading
from typing import Any, Callable

from chatbi.config import (
    TASK_STATUS_PENDING,
    TASK_STATUS_RUNNING,
    TASK_TYPE_REPORT_GENERATE,
    TASK_TYPE_SEMANTIC_REBUILD,
    TASK_WORKER_COUNT,
)
from chatbi.repository.task_repository import (
    create_task,
    get_task,
    list_pending_tasks,
    list_tasks,
    mark_task_failed,
    mark_task_progress,
    mark_task_running,
    mark_task_succeeded,
)
from chatbi.service.report_service import execute_report_generation_task
from semantic_layer import rebuild_admin_search

_TASK_QUEUE: queue.Queue[str] = queue.Queue()
_TASK_WORKERS_STARTED = False
_TASK_WORKERS_LOCK = threading.Lock()
_ENQUEUED_TASK_IDS: set[str] = set()


def _enqueue_task(task_id: str) -> None:
    if not task_id or task_id in _ENQUEUED_TASK_IDS:
        return
    _ENQUEUED_TASK_IDS.add(task_id)
    _TASK_QUEUE.put(task_id)


def start_task_workers() -> None:
    global _TASK_WORKERS_STARTED
    with _TASK_WORKERS_LOCK:
        if _TASK_WORKERS_STARTED:
            return
        if os.environ.get('WERKZEUG_RUN_MAIN') == 'false':
            return
        for task in list_pending_tasks(limit=100):
            _enqueue_task(task['task_id'])
        for index in range(max(1, TASK_WORKER_COUNT)):
            worker = threading.Thread(target=_task_worker_loop, name=f'chatbi-task-worker-{index+1}', daemon=True)
            worker.start()
        _TASK_WORKERS_STARTED = True


def submit_task(
    task_type: str,
    display_name: str,
    payload: dict[str, Any],
    *,
    conversation_id: str | None = None,
    client_id: str | None = None,
) -> dict[str, Any]:
    task = create_task(
        task_type,
        display_name,
        payload,
        conversation_id=conversation_id,
        client_id=client_id,
    )
    _enqueue_task(task['task_id'])
    return task


def get_task_view(task_id: str) -> dict[str, Any] | None:
    return get_task(task_id)


def list_task_views(*, client_id: str | None = None, conversation_id: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
    return list_tasks(client_id=client_id, conversation_id=conversation_id, limit=limit)


def _task_worker_loop() -> None:
    while True:
        task_id = _TASK_QUEUE.get()
        try:
            _process_task(task_id)
        finally:
            _ENQUEUED_TASK_IDS.discard(task_id)
            _TASK_QUEUE.task_done()


def _process_task(task_id: str) -> None:
    task = get_task(task_id)
    if not task or task['status'] not in {TASK_STATUS_PENDING, TASK_STATUS_RUNNING}:
        return
    mark_task_running(task_id)
    payload = task.get('payload') or {}

    def progress(percent: int, result: dict[str, Any] | None = None) -> None:
        mark_task_progress(task_id, percent, result)

    try:
        if task['task_type'] == TASK_TYPE_REPORT_GENERATE:
            result = execute_report_generation_task(payload, progress)
        elif task['task_type'] == TASK_TYPE_SEMANTIC_REBUILD:
            progress(20, {'step': '开始重建检索索引'})
            rebuild_result = rebuild_admin_search(refresh_embeddings=bool(payload.get('refresh_embeddings')))
            result = {
                'result': rebuild_result,
                'step': '完成',
            }
            progress(100, result)
        else:
            raise ValueError('不支持的异步任务类型')
        mark_task_succeeded(task_id, result)
    except Exception as exc:  # noqa: BLE001
        mark_task_failed(task_id, str(exc))
