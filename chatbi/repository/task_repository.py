import json
import re
import uuid
from typing import Any

from chatbi.config import (
    MAX_CLIENT_ID_LENGTH,
    TASK_POLL_LIMIT,
    TASK_STATUS_FAILED,
    TASK_STATUS_PENDING,
    TASK_STATUS_RUNNING,
    TASK_STATUS_SUCCEEDED,
)
from chatbi.repository.chat_repository import normalize_conversation_id
from chatbi.repository.db import get_db_conn


def normalize_client_id(raw_value: Any) -> str:
    text = str(raw_value or '').strip()
    if not text:
        return ''
    return text[:MAX_CLIENT_ID_LENGTH]


def normalize_worker_id(raw_value: Any) -> str:
    text = str(raw_value or '').strip()
    if not text:
        return ''
    return text[:120]


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


def _extract_json_object(text: str) -> dict[str, Any]:
    content = str(text or '').strip()
    if not content:
        return {}
    code_block = re.search(r'```(?:json)?\s*(\{.*\})\s*```', content, re.IGNORECASE | re.DOTALL)
    if code_block:
        content = code_block.group(1).strip()
    elif not content.startswith('{'):
        json_match = re.search(r'(\{.*\})', content, re.DOTALL)
        if json_match:
            content = json_match.group(1).strip()
    try:
        payload = json.loads(content)
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


def _extract_query_preview(request_json: Any) -> str:
    payload = _loads_json(request_json)
    messages = payload.get('messages')
    if not isinstance(messages, list):
        return ''
    for message in reversed(messages):
        if not isinstance(message, dict) or str(message.get('role') or '') != 'user':
            continue
        content = str(message.get('content') or '').strip()
        if not content:
            continue
        for pattern in [r'当前用户问题:\s*(.+)', r'当前问题:\s*(.+)']:
            match = re.search(pattern, content, re.DOTALL)
            if match:
                return str(match.group(1)).strip().splitlines()[0][:160]
        return content.splitlines()[0][:160]
    return ''


def _summarize_task_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    chart_images = normalized.get('chart_images')
    if isinstance(chart_images, list):
        normalized['chart_images'] = [
            {
                'title': str(item.get('title') or '图表快照'),
                'caption': str(item.get('caption') or ''),
                'png_data_url': f"[图片数据，已省略，长度 {len(str(item.get('png_data_url') or ''))} 字符]",
            }
            for item in chart_images
            if isinstance(item, dict)
        ]
    return normalized


def _normalize_task_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        'task_id': row['task_id'],
        'task_type': row['task_type'],
        'conversation_id': row.get('conversation_id') or '',
        'client_id': row.get('client_id') or '',
        'display_name': row.get('display_name') or '',
        'status': row.get('status') or '',
        'progress': int(row.get('progress') or 0),
        'attempt_count': int(row.get('attempt_count') or 0),
        'worker_id': row.get('worker_id') or '',
        'claim_token': row.get('claim_token') or '',
        'lease_expires_at': str(row.get('lease_expires_at') or ''),
        'payload': _summarize_task_payload(_loads_json(row.get('payload_json'))),
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


def requeue_expired_tasks(limit: int) -> int:
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE `async_task`
                SET `status` = %s,
                    `worker_id` = NULL,
                    `claim_token` = NULL,
                    `lease_expires_at` = NULL,
                    `updated_at` = NOW()
                WHERE `status` = %s
                  AND `lease_expires_at` IS NOT NULL
                  AND `lease_expires_at` < NOW()
                ORDER BY `updated_at` ASC
                LIMIT %s
                """,
                (TASK_STATUS_PENDING, TASK_STATUS_RUNNING, int(limit)),
            )
            affected = cursor.rowcount
        conn.commit()
    return affected


def claim_next_task(worker_id: str, lease_seconds: int) -> dict[str, Any] | None:
    worker_id = normalize_worker_id(worker_id)
    if not worker_id:
        raise ValueError('worker_id 不能为空')
    claim_token = uuid.uuid4().hex
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE `async_task`
                SET `status` = %s,
                    `progress` = CASE WHEN `progress` < 5 THEN 5 ELSE `progress` END,
                    `attempt_count` = `attempt_count` + 1,
                    `worker_id` = %s,
                    `claim_token` = %s,
                    `error_message` = NULL,
                    `finished_at` = NULL,
                    `started_at` = COALESCE(`started_at`, NOW()),
                    `lease_expires_at` = DATE_ADD(NOW(), INTERVAL %s SECOND),
                    `updated_at` = NOW()
                WHERE `status` = %s
                ORDER BY `created_at` ASC, `task_id` ASC
                LIMIT 1
                """,
                (TASK_STATUS_RUNNING, worker_id, claim_token, int(lease_seconds), TASK_STATUS_PENDING),
            )
            affected = cursor.rowcount
            if not affected:
                conn.commit()
                return None
            cursor.execute('SELECT * FROM `async_task` WHERE `claim_token` = %s LIMIT 1', (claim_token,))
            row = cursor.fetchone()
        conn.commit()
    return _normalize_task_row(row) if row else None


def heartbeat_task(task_id: str, worker_id: str, lease_seconds: int, progress: int | None = None, result: dict[str, Any] | None = None) -> None:
    worker_id = normalize_worker_id(worker_id)
    assignments = [
        '`lease_expires_at` = DATE_ADD(NOW(), INTERVAL %s SECOND)',
        '`updated_at` = NOW()',
    ]
    params: list[Any] = [int(lease_seconds)]
    if progress is not None:
        assignments.append('`progress` = %s')
        params.append(max(0, min(100, int(progress))))
    if result is not None:
        assignments.append('`result_json` = %s')
        params.append(json.dumps(result, ensure_ascii=False, default=str))
    params.extend([task_id, worker_id])
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE `async_task`
                SET {', '.join(assignments)}
                WHERE `task_id` = %s AND `worker_id` = %s
                """,
                tuple(params),
            )
        conn.commit()


def mark_task_progress(task_id: str, progress: int, result: dict[str, Any] | None = None, *, worker_id: str | None = None, lease_seconds: int | None = None) -> None:
    if worker_id and lease_seconds:
        heartbeat_task(task_id, worker_id, lease_seconds, progress=progress, result=result)
        return
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
                    `worker_id` = NULL,
                    `claim_token` = NULL,
                    `lease_expires_at` = NULL,
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
                    `worker_id` = NULL,
                    `claim_token` = NULL,
                    `lease_expires_at` = NULL,
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
    request_id: str | None,
    round_no: int | None,
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
                (`conversation_id`, `client_id`, `request_id`, `round_no`, `stage`, `llm_provider`, `model_name`, `request_json`, `response_json`, `error_message`)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    normalized_conversation_id,
                    normalized_client_id or None,
                    str(request_id or '').strip()[:64] or None,
                    int(round_no) if round_no else None,
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
                       `request_id`, `round_no`, `request_json`, `response_json`, `error_message`, `created_at`
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
                'request_id': row.get('request_id') or '',
                'round_no': int(row.get('round_no') or 0),
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


def get_latest_task_by_type(task_type: str) -> dict[str, Any] | None:
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM `async_task`
                WHERE `task_type` = %s
                ORDER BY `created_at` DESC, `task_id` DESC
                LIMIT 1
                """,
                (task_type,),
            )
            row = cursor.fetchone()
    return _normalize_task_row(row) if row else None


def get_query_plan_quality_stats(limit: int = 200) -> dict[str, Any]:
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT `id`, `conversation_id`, `request_id`, `round_no`, `llm_provider`, `model_name`,
                       `request_json`, `response_json`, `error_message`, `created_at`
                FROM `llm_invocation_log`
                WHERE `stage` = 'query_plan'
                ORDER BY `id` DESC
                LIMIT %s
                """,
                (int(limit),),
            )
            rows = list(cursor.fetchall())

    stats = {
        'sample_size': len(rows),
        'query_count': 0,
        'clarify_count': 0,
        'invalid_json_count': 0,
        'missing_metric_definition_count': 0,
        'missing_metric_description_count': 0,
        'missing_metrics_count': 0,
        'missing_sql_count': 0,
        'issue_count': 0,
        'recent_issues': [],
    }

    for row in rows:
        response_wrapper = _loads_json(row.get('response_json'))
        response_payload = _extract_json_object(response_wrapper.get('content'))
        if not response_payload:
            stats['invalid_json_count'] += 1
            stats['issue_count'] += 1
            stats['recent_issues'].append(
                {
                    'created_at': str(row.get('created_at') or ''),
                    'conversation_id': row.get('conversation_id') or '',
                    'request_id': row.get('request_id') or '',
                    'round_no': int(row.get('round_no') or 0),
                    'llm_provider': row.get('llm_provider') or '',
                    'model_name': row.get('model_name') or '',
                    'question_preview': _extract_query_preview(row.get('request_json')),
                    'missing_fields': ['响应不是有效 JSON'],
                    'error_message': row.get('error_message') or '',
                }
            )
            continue

        action = str(response_payload.get('action', 'query')).strip().lower()
        if action == 'clarify':
            stats['clarify_count'] += 1
            continue

        stats['query_count'] += 1
        missing_fields: list[str] = []
        if not str(response_payload.get('metric_definition', '')).strip():
            stats['missing_metric_definition_count'] += 1
            missing_fields.append('metric_definition')
        if not str(response_payload.get('metric_description', '')).strip():
            stats['missing_metric_description_count'] += 1
            missing_fields.append('metric_description')
        metrics = response_payload.get('metrics')
        if not isinstance(metrics, list) or not [str(item).strip() for item in metrics if str(item).strip()]:
            stats['missing_metrics_count'] += 1
            missing_fields.append('metrics')
        if not str(response_payload.get('sql', '')).strip():
            stats['missing_sql_count'] += 1
            missing_fields.append('sql')
        if missing_fields:
            stats['issue_count'] += 1
            stats['recent_issues'].append(
                {
                    'created_at': str(row.get('created_at') or ''),
                    'conversation_id': row.get('conversation_id') or '',
                    'request_id': row.get('request_id') or '',
                    'round_no': int(row.get('round_no') or 0),
                    'llm_provider': row.get('llm_provider') or '',
                    'model_name': row.get('model_name') or '',
                    'question_preview': _extract_query_preview(row.get('request_json')),
                    'missing_fields': missing_fields,
                    'error_message': row.get('error_message') or '',
                }
            )

    stats['recent_issues'] = stats['recent_issues'][:10]
    return stats
