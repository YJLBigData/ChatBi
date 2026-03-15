import json
import re
from typing import Any

from chatbi.config import MAX_CONVERSATION_ID_LENGTH, MAX_HISTORY_MESSAGES, MAX_UI_HISTORY_MESSAGES
from chatbi.repository.db import get_db_conn


def normalize_conversation_id(raw_value: Any) -> str:
    value = re.sub(r'[^a-zA-Z0-9_-]+', '_', str(raw_value or '').strip())
    return (value or 'default')[:MAX_CONVERSATION_ID_LENGTH]


def ensure_chat_session(conversation_id: str, title: str | None = None) -> None:
    conversation_id = normalize_conversation_id(conversation_id)
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT IGNORE INTO `chat_session` (`conversation_id`, `title`)
                VALUES (%s, %s)
                """,
                (conversation_id, title or None),
            )
            if title:
                cursor.execute(
                    """
                    UPDATE `chat_session`
                    SET `title` = CASE WHEN `title` IS NULL OR `title` = '' THEN %s ELSE `title` END,
                        `updated_at` = NOW()
                    WHERE `conversation_id` = %s
                    """,
                    (title, conversation_id),
                )
        conn.commit()


def get_chat_session_row(conversation_id: str) -> dict[str, Any] | None:
    conversation_id = normalize_conversation_id(conversation_id)
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT `conversation_id`, `title`, `latest_result_json`, `context_summary`,
                       `summary_message_count`, `last_compacted_message_id`, `context_stats_json`
                FROM `chat_session`
                WHERE `conversation_id` = %s
                """,
                (conversation_id,),
            )
            return cursor.fetchone()


def update_chat_session_context(
    conversation_id: str,
    *,
    context_summary: str | None = None,
    summary_message_count: int | None = None,
    last_compacted_message_id: int | None = None,
    context_stats: dict[str, Any] | None = None,
) -> None:
    conversation_id = normalize_conversation_id(conversation_id)
    ensure_chat_session(conversation_id)
    assignments: list[str] = []
    params: list[Any] = []
    if context_summary is not None:
        assignments.append('`context_summary` = %s')
        params.append(context_summary)
    if summary_message_count is not None:
        assignments.append('`summary_message_count` = %s')
        params.append(summary_message_count)
    if last_compacted_message_id is not None:
        assignments.append('`last_compacted_message_id` = %s')
        params.append(last_compacted_message_id)
    if context_stats is not None:
        assignments.append('`context_stats_json` = %s')
        params.append(json.dumps(context_stats, ensure_ascii=False, default=str))
    if not assignments:
        return
    assignments.append('`updated_at` = NOW()')
    params.append(conversation_id)
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"UPDATE `chat_session` SET {', '.join(assignments)} WHERE `conversation_id` = %s",
                params,
            )
        conn.commit()


def get_conversation_history_records(conversation_id: str, limit: int = MAX_HISTORY_MESSAGES) -> list[dict[str, Any]]:
    conversation_id = normalize_conversation_id(conversation_id)
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT `id`, `role`, `content`, COALESCE(`display_content`, `content`) AS `display_content`, `created_at`
                FROM `chat_message`
                WHERE `conversation_id` = %s
                ORDER BY `id` DESC
                LIMIT %s
                """,
                (conversation_id, limit),
            )
            rows = list(cursor.fetchall())
    rows.reverse()
    return rows


def get_conversation_messages_for_ui(conversation_id: str) -> list[dict[str, str]]:
    rows = get_conversation_history_records(conversation_id, MAX_UI_HISTORY_MESSAGES)
    return [{'role': row['role'], 'content': row['display_content']} for row in rows]


def append_conversation_message(
    conversation_id: str,
    role: str,
    content: str,
    display_content: str | None = None,
) -> None:
    conversation_id = normalize_conversation_id(conversation_id)
    ensure_chat_session(conversation_id)
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO `chat_message` (`conversation_id`, `role`, `content`, `display_content`)
                VALUES (%s, %s, %s, %s)
                """,
                (conversation_id, role, content, display_content or content),
            )
            cursor.execute(
                "UPDATE `chat_session` SET `updated_at` = NOW() WHERE `conversation_id` = %s",
                (conversation_id,),
            )
        conn.commit()


def save_latest_result_json(conversation_id: str, result_payload: dict[str, Any]) -> None:
    conversation_id = normalize_conversation_id(conversation_id)
    ensure_chat_session(conversation_id)
    payload_json = json.dumps(result_payload, ensure_ascii=False, default=str)
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE `chat_session`
                SET `latest_result_json` = %s,
                    `updated_at` = NOW()
                WHERE `conversation_id` = %s
                """,
                (payload_json, conversation_id),
            )
        conn.commit()
