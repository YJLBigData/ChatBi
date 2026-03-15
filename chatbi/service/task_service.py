import os
import socket
import threading
import time
import logging
from typing import Any

from chatbi.config import (
    TASK_HEARTBEAT_INTERVAL_SECONDS,
    TASK_LEASE_SECONDS,
    TASK_STALE_BATCH_SIZE,
    TASK_TYPE_REPORT_GENERATE,
    TASK_TYPE_SEMANTIC_REBUILD,
    TASK_WORKER_POLL_INTERVAL_SECONDS,
)
from chatbi.repository.task_repository import (
    claim_next_task,
    create_task,
    get_task,
    heartbeat_task,
    list_tasks,
    mark_task_failed,
    mark_task_progress,
    mark_task_succeeded,
    requeue_expired_tasks,
)
from chatbi.service.report_service import execute_report_generation_task
from semantic_layer import rebuild_admin_search

logger = logging.getLogger(__name__)


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
    logger.info(
        'task submitted task_id=%s task_type=%s conversation_id=%s client_id=%s display_name=%s',
        task.get('task_id'),
        task_type,
        conversation_id or '',
        client_id or '',
        display_name,
    )
    return task


def get_task_view(task_id: str) -> dict[str, Any] | None:
    return get_task(task_id)


def list_task_views(*, client_id: str | None = None, conversation_id: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
    return list_tasks(client_id=client_id, conversation_id=conversation_id, limit=limit)


def build_worker_id(name: str | None = None) -> str:
    if name:
        return name[:120]
    hostname = socket.gethostname().split('.')[0]
    return f'worker_{hostname}_{os.getpid()}'


def execute_task(task: dict[str, Any], worker_id: str) -> dict[str, Any]:
    task_id = task['task_id']
    payload = task.get('payload') or {}
    logger.info('task execution started task_id=%s task_type=%s worker_id=%s', task_id, task['task_type'], worker_id)

    def progress(percent: int, result: dict[str, Any] | None = None) -> None:
        mark_task_progress(
            task_id,
            percent,
            result,
            worker_id=worker_id,
            lease_seconds=TASK_LEASE_SECONDS,
        )

    if task['task_type'] == TASK_TYPE_REPORT_GENERATE:
        return execute_report_generation_task(payload, progress)
    if task['task_type'] == TASK_TYPE_SEMANTIC_REBUILD:
        progress(20, {'step': '开始重建检索索引'})
        rebuild_result = rebuild_admin_search(refresh_embeddings=bool(payload.get('refresh_embeddings')))
        progress(100, {'result': rebuild_result, 'step': '完成'})
        return {
            'result': rebuild_result,
            'step': '完成',
        }
    raise ValueError('不支持的异步任务类型')


def _heartbeat_loop(task_id: str, worker_id: str, stop_event: threading.Event) -> None:
    while not stop_event.wait(TASK_HEARTBEAT_INTERVAL_SECONDS):
        heartbeat_task(task_id, worker_id, TASK_LEASE_SECONDS)


def process_claimed_task(task: dict[str, Any], worker_id: str) -> None:
    task_id = task['task_id']
    stop_event = threading.Event()
    heartbeat_thread = threading.Thread(
        target=_heartbeat_loop,
        args=(task_id, worker_id, stop_event),
        name=f'heartbeat-{task_id}',
        daemon=True,
    )
    heartbeat_thread.start()
    try:
        result = execute_task(task, worker_id)
        mark_task_succeeded(task_id, result)
        logger.info('task execution succeeded task_id=%s worker_id=%s', task_id, worker_id)
    except Exception as exc:  # noqa: BLE001
        mark_task_failed(task_id, str(exc))
        logger.exception('task execution failed task_id=%s worker_id=%s error=%s', task_id, worker_id, exc)
    finally:
        stop_event.set()
        heartbeat_thread.join(timeout=max(1, TASK_HEARTBEAT_INTERVAL_SECONDS))


def run_task_worker_forever(
    *,
    worker_id: str | None = None,
    poll_interval: float = TASK_WORKER_POLL_INTERVAL_SECONDS,
) -> None:
    resolved_worker_id = build_worker_id(worker_id)
    logger.info('task worker loop started worker_id=%s poll_interval=%s', resolved_worker_id, poll_interval)
    while True:
        try:
            requeue_expired_tasks(TASK_STALE_BATCH_SIZE)
            task = claim_next_task(resolved_worker_id, TASK_LEASE_SECONDS)
            if not task:
                time.sleep(max(0.5, poll_interval))
                continue
            logger.info('task claimed task_id=%s task_type=%s worker_id=%s', task['task_id'], task['task_type'], resolved_worker_id)
            process_claimed_task(task, resolved_worker_id)
        except Exception as exc:  # noqa: BLE001
            logger.exception('task worker loop error worker_id=%s error=%s', resolved_worker_id, exc)
            time.sleep(max(1, poll_interval))
