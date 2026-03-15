import json
import uuid
from typing import Any

from chatbi.config import TASK_POLL_LIMIT, TASK_STATUS_FAILED, TASK_STATUS_PENDING, TASK_STATUS_RUNNING, TASK_STATUS_SUCCEEDED
from chatbi.repository.chat_repository import normalize_conversation_id
from chatbi.repository.db import get_db_conn


def normalize_client_id(raw_value: Any) -> str:
    text = str(raw_value or '').strip()
    if not text:
        return ''
    return text[:80]


def _loads_json(raw_value: Any) -> dict[str, Any]:
    if not raw_value:
        return {}
    if isinstance(raw_value, dict):
        return raw_value
    try:
        payload = json.loads(str(raw_value))
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


def _normalize_task_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        'task_id': row['task_id'],
        'task_type': row['task_type'],
        'conversation_id': row.get('conversation_id') or '',
        'client_id': row.get('client_id') or '',
        'display_name': row.get('display_name') or '',
        'status': row.get('status') or '',
        'progress': int(row.get('progress') or 0),
        'payload': _loads_json(row.get('payload_json')),
        'result': _loads_json(row.get('result_json')),
        'error_message': row.get('error_message') or '',
        'created_at': str(row.get('created_at') or ''),
        'started_at': str(row.get('started_at') or ''),
        'finished_at': str(row.get('finished_at') or ''),
        'updated_at': str(row.get('updated_at') or ''),
    }


def create_task(
    task_type: str,
    display_name: str,
    payload: dict[str, Any],
    *,
    conversation_id: str | None = None,
    client_id: str | None = None,
) -> dict[str, Any]:
    task_id = f'task_{uuid.uuid4().hex[:18]}'
    conversation_id = normalize_conversation_id(conversation_id) if conversation_id else None
    client_id = normalize_client_id(client_id)
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO `async_task`
                (`task_id`, `task_type`, `conversation_id`, `client_id`, `display_name`, `status`, `progress`, `payload_json`)
                VALUES (%s, %s, %s, %s, %s, %s, 0, %s)
                """,
                (
                    task_id,
                    task_type,
                    conversation_id,
                    client_id or None,
                    display_name,
                    TASK_STATUS_PENDING,
                    json.dumps(payload, ensure_ascii=False, default=str),
                ),
            )
        conn.commit()
    return get_task(task_id)


def get_task(task_id: str) -> dict[str, Any] | None:
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT * FROM `async_task` WHERE `task_id` = %s', (task_id,))
            row = cursor.fetchone()
    return _normalize_task_row(row) if row else None


def list_tasks(*, client_id: str | None = None, conversation_id: str | None = None, limit: int = TASK_POLL_LIMIT) -> list[dict[str, Any]]:
    clauses = []
    params: list[Any] = []
    normalized_client_id = normalize_client_id(client_id)
    normalized_conversation_id = normalize_conversation_id(conversation_id) if conversation_id else ''
    if normalized_client_id:
        clauses.append('`client_id` = %s')
        params.append(normalized_client_id)
    if normalized_conversation_id:
        clauses.append('`conversation_id` = %s')
        params.append(normalized_conversation_id)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ''
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT * FROM `async_task` {where_sql} ORDER BY `created_at` DESC, `task_id` DESC LIMIT %s",
                tuple(params + [int(limit)]),
            )
            rows = list(cursor.fetchall())
    return [_normalize_task_row(row) for row in rows]


def list_pending_tasks(limit: int = 50) -> list[dict[str, Any]]:
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM `async_task`
                WHERE `status` IN (%s, %s)
                ORDER BY `created_at` ASC
                LIMIT %s
                """,
                (TASK_STATUS_PENDING, TASK_STATUS_RUNNING, int(limit)),
            )
            rows = list(cursor.fetchall())
    return [_normalize_task_row(row) for row in rows]


def mark_task_running(task_id: str) -> None:
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE `async_task`
                SET `status` = %s,
                    `progress` = CASE WHEN `progress` < 5 THEN 5 ELSE `progress` END,
                    `started_at` = COALESCE(`started_at`, NOW()),
                    `updated_at` = NOW()
                WHERE `task_id` = %s
                """,
                (TASK_STATUS_RUNNING, task_id),
            )
        conn.commit()


def mark_task_progress(task_id: str, progress: int, result: dict[str, Any] | None = None) -> None:
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE `async_task`
                SET `progress` = %s,
                    `result_json` = CASE WHEN %s IS NULL THEN `result_json` ELSE %s END,
                    `updated_at` = NOW()
                WHERE `task_id` = %s
                """,
                (
                    max(0, min(100, int(progress))),
                    None if result is None else 1,
                    json.dumps(result, ensure_ascii=False, default=str) if result is not None else None,
                    task_id,
                ),
            )
        conn.commit()


def mark_task_succeeded(task_id: str, result: dict[str, Any]) -> None:
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE `async_task`
                SET `status` = %s,
                    `progress` = 100,
                    `result_json` = %s,
                    `error_message` = NULL,
                    `finished_at` = NOW(),
                    `updated_at` = NOW()
                WHERE `task_id` = %s
                """,
                (TASK_STATUS_SUCCEEDED, json.dumps(result, ensure_ascii=False, default=str), task_id),
            )
        conn.commit()


def mark_task_failed(task_id: str, error_message: str) -> None:
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE `async_task`
                SET `status` = %s,
                    `error_message` = %s,
                    `finished_at` = NOW(),
                    `updated_at` = NOW()
                WHERE `task_id` = %s
                """,
                (TASK_STATUS_FAILED, str(error_message or ''), task_id),
            )
        conn.commit()


def insert_llm_invocation_log(
    *,
    conversation_id: str | None,
    client_id: str | None,
    stage: str,
    llm_provider: str,
    model_name: str,
    request_payload: dict[str, Any],
    response_payload: Any = None,
    error_message: str | None = None,
) -> None:
    normalized_conversation_id = normalize_conversation_id(conversation_id) if conversation_id else None
    normalized_client_id = normalize_client_id(client_id)
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO `llm_invocation_log`
                (`conversation_id`, `client_id`, `stage`, `llm_provider`, `model_name`, `request_json`, `response_json`, `error_message`)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    normalized_conversation_id,
                    normalized_client_id or None,
                    stage,
                    llm_provider,
                    model_name,
                    json.dumps(request_payload, ensure_ascii=False, default=str, indent=2),
                    json.dumps(response_payload, ensure_ascii=False, default=str, indent=2) if response_payload is not None else None,
                    error_message,
                ),
            )
        conn.commit()


def list_llm_invocation_logs(conversation_id: str, limit: int = 200) -> list[dict[str, Any]]:
    normalized_conversation_id = normalize_conversation_id(conversation_id)
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT `id`, `conversation_id`, `client_id`, `stage`, `llm_provider`, `model_name`,
                       `request_json`, `response_json`, `error_message`, `created_at`
                FROM `llm_invocation_log`
                WHERE `conversation_id` = %s
                ORDER BY `id` DESC
                LIMIT %s
                """,
                (normalized_conversation_id, int(limit)),
            )
            rows = list(cursor.fetchall())
    rows.reverse()
    result = []
    for row in rows:
        result.append(
            {
                'id': row['id'],
                'conversation_id': row['conversation_id'],
                'client_id': row.get('client_id') or '',
                'stage': row['stage'],
                'llm_provider': row['llm_provider'],
                'model_name': row['model_name'],
                'request_json': row['request_json'],
                'response_json': row.get('response_json') or '',
                'error_message': row.get('error_message') or '',
                'created_at': str(row['created_at']),
            }
        )
    return result
