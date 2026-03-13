import json
import math
import os
import re
from datetime import date
from typing import Any

import pymysql
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
from openai import OpenAI

from semantic_layer import (
    ensure_semantic_runtime,
    get_admin_bootstrap,
    get_semantic_maintenance_guide,
    rebuild_admin_search,
    retrieve_semantic_context,
    sync_semantic_schema,
    upsert_admin_entity,
    delete_admin_entity,
)


load_dotenv()


app = Flask(__name__)


DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "toor"),
    "database": os.getenv("MYSQL_DATABASE", "test"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": False,
}

DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY", "")
DASHSCOPE_BASE_URL = os.getenv(
    "DASHSCOPE_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"
)
DASHSCOPE_MODEL = os.getenv("DASHSCOPE_MODEL", "qwen3-max")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-reasoner")
MAX_RESULT_ROWS = int(os.getenv("MAX_RESULT_ROWS", "200"))
MAX_HISTORY_MESSAGES = int(os.getenv("MAX_HISTORY_MESSAGES", "20"))
MAX_UI_HISTORY_MESSAGES = int(os.getenv("MAX_UI_HISTORY_MESSAGES", "100"))
MAX_CONTEXT_SOURCE_MESSAGES = int(os.getenv("MAX_CONTEXT_SOURCE_MESSAGES", "160"))
MAX_CONTEXT_RECENT_MESSAGES = int(os.getenv("MAX_CONTEXT_RECENT_MESSAGES", "16"))
CONTEXT_COMPRESSION_TRIGGER_MESSAGES = int(
    os.getenv("CONTEXT_COMPRESSION_TRIGGER_MESSAGES", "24")
)
CONTEXT_COMPRESSION_TRIGGER_TOKENS = int(
    os.getenv("CONTEXT_COMPRESSION_TRIGGER_TOKENS", "15000")
)
MAX_CONTEXT_SUMMARY_LINES = int(os.getenv("MAX_CONTEXT_SUMMARY_LINES", "10"))
QUERY_TIMEOUT_MS = int(os.getenv("QUERY_TIMEOUT_MS", "15000"))
MAX_CONVERSATION_ID_LENGTH = int(os.getenv("MAX_CONVERSATION_ID_LENGTH", "80"))
LLM_REQUEST_TIMEOUT_SECONDS = int(os.getenv("LLM_REQUEST_TIMEOUT_SECONDS", "90"))
ALLOWED_BASE_TABLES = {
    "order_master",
    "order_detail",
    "user_info",
    "product_info",
    "store_info",
    "refund_master",
    "refund_detail",
}
TODAY_STR = date.today().isoformat()
RUNTIME_READY = False

CONTEXT_STRATEGY_LABEL = "滚动摘要 + 最近窗口"
RUNTIME_BOOTSTRAP_LOCK_NAME = "chatbi_runtime_bootstrap"

LLM_PROVIDER_CONFIGS = {
    "bailian": {
        "label": "阿里百炼",
        "api_key": DASHSCOPE_API_KEY,
        "base_url": DASHSCOPE_BASE_URL,
        "model": DASHSCOPE_MODEL,
        "max_input_tokens": 258048,
    },
    "deepseek": {
        "label": "DeepSeek",
        "api_key": DEEPSEEK_API_KEY,
        "base_url": DEEPSEEK_BASE_URL,
        "model": DEEPSEEK_MODEL,
        "max_input_tokens": 128000,
    },
}

LLM_PROVIDER_ALIASES = {
    "bailian": "bailian",
    "aliyun": "bailian",
    "dashscope": "bailian",
    "qwen": "bailian",
    "deepseek": "deepseek",
    "ds": "deepseek",
}

LLM_PROVIDER_OPTIONS = [
    {"value": "bailian", "label": "阿里百炼", "model": DASHSCOPE_MODEL},
    {"value": "deepseek", "label": "DeepSeek", "model": DEEPSEEK_MODEL},
]

CHAT_SESSION_DDL = """
CREATE TABLE IF NOT EXISTS `chat_session` (
    `conversation_id` VARCHAR(80) NOT NULL COMMENT '会话ID',
    `title` VARCHAR(255) NULL COMMENT '会话标题',
    `latest_result_json` LONGTEXT NULL COMMENT '最近一次查询结果快照',
    `context_summary` LONGTEXT NULL COMMENT '滚动上下文摘要',
    `summary_message_count` INT NOT NULL DEFAULT 0 COMMENT '已压缩消息数',
    `last_compacted_message_id` BIGINT NULL COMMENT '最近一次压缩到的消息ID',
    `context_stats_json` LONGTEXT NULL COMMENT '上下文统计快照',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`conversation_id`),
    KEY `idx_chat_session_updated_at` (`updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ChatBI会话主表';
"""

CHAT_MESSAGE_DDL = """
CREATE TABLE IF NOT EXISTS `chat_message` (
    `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '消息ID',
    `conversation_id` VARCHAR(80) NOT NULL COMMENT '会话ID',
    `role` VARCHAR(20) NOT NULL COMMENT '角色',
    `content` LONGTEXT NOT NULL COMMENT '模型上下文内容',
    `display_content` LONGTEXT NULL COMMENT '页面展示内容',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (`id`),
    KEY `idx_chat_message_conversation` (`conversation_id`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ChatBI会话消息表';
"""

CHAT_SESSION_MIGRATIONS = {
    "title": "ALTER TABLE `chat_session` ADD COLUMN `title` VARCHAR(255) NULL COMMENT '会话标题' AFTER `conversation_id`",
    "latest_result_json": "ALTER TABLE `chat_session` ADD COLUMN `latest_result_json` LONGTEXT NULL COMMENT '最近一次查询结果快照' AFTER `title`",
    "context_summary": "ALTER TABLE `chat_session` ADD COLUMN `context_summary` LONGTEXT NULL COMMENT '滚动上下文摘要' AFTER `latest_result_json`",
    "summary_message_count": "ALTER TABLE `chat_session` ADD COLUMN `summary_message_count` INT NOT NULL DEFAULT 0 COMMENT '已压缩消息数' AFTER `context_summary`",
    "last_compacted_message_id": "ALTER TABLE `chat_session` ADD COLUMN `last_compacted_message_id` BIGINT NULL COMMENT '最近一次压缩到的消息ID' AFTER `summary_message_count`",
    "context_stats_json": "ALTER TABLE `chat_session` ADD COLUMN `context_stats_json` LONGTEXT NULL COMMENT '上下文统计快照' AFTER `last_compacted_message_id`",
}

CHAT_MESSAGE_MIGRATIONS = {
    "display_content": "ALTER TABLE `chat_message` ADD COLUMN `display_content` LONGTEXT NULL COMMENT '页面展示内容' AFTER `content`",
}


def normalize_llm_provider(raw_value: Any) -> str:
    value = str(raw_value or "").strip().lower()
    return LLM_PROVIDER_ALIASES.get(value, "")


def resolve_default_llm_provider() -> str:
    configured = normalize_llm_provider(os.getenv("DEFAULT_LLM_PROVIDER", "bailian")) or "bailian"
    if LLM_PROVIDER_CONFIGS.get(configured, {}).get("api_key"):
        return configured
    for provider_name in ["bailian", "deepseek"]:
        if LLM_PROVIDER_CONFIGS.get(provider_name, {}).get("api_key"):
            return provider_name
    return configured


DEFAULT_LLM_PROVIDER = resolve_default_llm_provider()


def get_llm_provider_meta(provider_name: str | None = None) -> dict[str, Any]:
    resolved_provider = normalize_llm_provider(provider_name) or DEFAULT_LLM_PROVIDER
    config = LLM_PROVIDER_CONFIGS.get(resolved_provider) or LLM_PROVIDER_CONFIGS[DEFAULT_LLM_PROVIDER]
    return {
        "provider": resolved_provider,
        "label": config["label"],
        "model": config["model"],
        "max_input_tokens": config["max_input_tokens"],
    }


def get_llm_runtime(provider_name: str | None = None) -> dict[str, Any]:
    meta = get_llm_provider_meta(provider_name)
    resolved_provider = meta["provider"]
    config = LLM_PROVIDER_CONFIGS.get(resolved_provider)
    if not config:
        raise ValueError("不支持的模型引擎")
    if not config.get("api_key"):
        env_name = "DASHSCOPE_API_KEY" if resolved_provider == "bailian" else "DEEPSEEK_API_KEY"
        raise ValueError(f"缺少 {env_name}，请先在 .env 中配置")
    return {
        **meta,
        "client": OpenAI(
            api_key=config["api_key"],
            base_url=config["base_url"],
            timeout=LLM_REQUEST_TIMEOUT_SECONDS,
            max_retries=1,
        ),
    }


def get_db_conn() -> pymysql.connections.Connection:
    try:
        return pymysql.connect(**DB_CONFIG)
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        if "cryptography" in message.lower():
            raise ValueError(
                "当前 MySQL 认证方式需要 cryptography 依赖。requirements.txt 已加入该依赖，请执行 pip install -r requirements.txt"
            ) from exc
        raise


def ensure_table_columns(
    cursor: pymysql.cursors.DictCursor,
    table_name: str,
    expected_migrations: dict[str, str],
) -> None:
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    existing_columns = {
        row.get("Field") or row.get("field")
        for row in cursor.fetchall()
        if (row.get("Field") or row.get("field"))
    }
    for column_name, ddl in expected_migrations.items():
        if column_name not in existing_columns:
            cursor.execute(ddl)


def normalize_conversation_id(raw_value: Any) -> str:
    value = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(raw_value or "").strip())
    return (value or "default")[:MAX_CONVERSATION_ID_LENGTH]


def acquire_runtime_lock(conn: pymysql.connections.Connection) -> None:
    with conn.cursor() as cursor:
        cursor.execute("SELECT GET_LOCK(%s, 30) AS `lock_status`", (RUNTIME_BOOTSTRAP_LOCK_NAME,))
        row = cursor.fetchone() or {}
    if int(row.get("lock_status") or 0) != 1:
        raise ValueError("启动运行时初始化失败：未能获取系统引导锁")


def release_runtime_lock(conn: pymysql.connections.Connection) -> None:
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT RELEASE_LOCK(%s)", (RUNTIME_BOOTSTRAP_LOCK_NAME,))
    except Exception:  # noqa: BLE001
        pass


def ensure_runtime_ready() -> None:
    global RUNTIME_READY
    if RUNTIME_READY:
        return

    with get_db_conn() as conn:
        acquire_runtime_lock(conn)
        try:
            with conn.cursor() as cursor:
                cursor.execute(CHAT_SESSION_DDL)
                cursor.execute(CHAT_MESSAGE_DDL)
                ensure_table_columns(cursor, "chat_session", CHAT_SESSION_MIGRATIONS)
                ensure_table_columns(cursor, "chat_message", CHAT_MESSAGE_MIGRATIONS)
            conn.commit()
            ensure_semantic_runtime(refresh_embeddings=False)
        finally:
            release_runtime_lock(conn)
    RUNTIME_READY = True


def extract_json_payload(text: str) -> dict[str, Any]:
    content = text.strip()
    code_block = re.search(r"```(?:json)?\s*(\{.*\})\s*```", content, re.IGNORECASE | re.DOTALL)
    if code_block:
        content = code_block.group(1).strip()
    elif not content.startswith("{"):
        json_match = re.search(r"(\{.*\})", content, re.DOTALL)
        if json_match:
            content = json_match.group(1).strip()

    payload = json.loads(content)
    if not isinstance(payload, dict):
        raise ValueError("模型返回格式错误，未得到 JSON 对象")
    return payload


def safe_load_json_dict(raw_value: Any) -> dict[str, Any]:
    if not raw_value:
        return {}
    if isinstance(raw_value, dict):
        return raw_value
    try:
        payload = json.loads(str(raw_value))
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


def estimate_text_tokens(text: str) -> int:
    content = str(text or "")
    chinese_char_count = len(re.findall(r"[\u4e00-\u9fff]", content))
    word_count = len(re.findall(r"[A-Za-z0-9_]+", content))
    punctuation_count = len(re.findall(r"[^\w\s\u4e00-\u9fff]", content))
    whitespace_bonus = max(1, len(content) // 80)
    return max(1, chinese_char_count + math.ceil(word_count * 1.3) + math.ceil(punctuation_count * 0.3) + whitespace_bonus)


def estimate_message_tokens(messages: list[dict[str, str]]) -> int:
    return sum(estimate_text_tokens(f"{message.get('role', '')}:{message.get('content', '')}") + 4 for message in messages)


def compact_text(text: str, limit: int = 160) -> str:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[: limit - 1].rstrip()}…"


def normalize_summary_text(text: str) -> str:
    lines: list[str] = []
    for raw_line in str(text or "").splitlines():
        cleaned = re.sub(r"^\s*[-*•\d\.\)\(]+\s*", "", raw_line).strip()
        if not cleaned:
            continue
        normalized = f"- {compact_text(cleaned, 140)}"
        if normalized not in lines:
            lines.append(normalized)
        if len(lines) >= MAX_CONTEXT_SUMMARY_LINES:
            break
    return "\n".join(lines)


def format_history_lines(history: list[dict[str, str]], max_messages: int | None = None) -> str:
    if not history:
        return "无历史对话"
    formatted = []
    messages = history[-max_messages:] if max_messages else history
    for message in messages:
        role_name = "用户" if message["role"] == "user" else "助手"
        formatted.append(f"{role_name}: {message['content']}")
    return "\n".join(formatted)


def build_fallback_summary(existing_summary: str, records: list[dict[str, Any]]) -> str:
    base_lines = normalize_summary_text(existing_summary).splitlines() if existing_summary else []
    new_lines: list[str] = []
    for record in records[-MAX_CONTEXT_SUMMARY_LINES:]:
        role_name = "用户" if record.get("role") == "user" else "助手"
        snippet = compact_text(record.get("content", ""), 120)
        if snippet:
            new_lines.append(f"- {role_name}: {snippet}")
    merged_lines: list[str] = []
    for line in base_lines + new_lines:
        if line and line not in merged_lines:
            merged_lines.append(line)
        if len(merged_lines) >= MAX_CONTEXT_SUMMARY_LINES:
            break
    return "\n".join(merged_lines)


def extract_cte_names(sql: str) -> set[str]:
    return set(re.findall(r"(?:(?:with)|,)\s*([a-zA-Z_][\w]*)\s+as\s*\(", sql, re.IGNORECASE))


def ensure_chat_session(conversation_id: str, title: str | None = None) -> None:
    conversation_id = normalize_conversation_id(conversation_id)
    ensure_runtime_ready()
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
    ensure_runtime_ready()
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
        assignments.append("`context_summary` = %s")
        params.append(context_summary)
    if summary_message_count is not None:
        assignments.append("`summary_message_count` = %s")
        params.append(summary_message_count)
    if last_compacted_message_id is not None:
        assignments.append("`last_compacted_message_id` = %s")
        params.append(last_compacted_message_id)
    if context_stats is not None:
        assignments.append("`context_stats_json` = %s")
        params.append(json.dumps(context_stats, ensure_ascii=False, default=str))

    if not assignments:
        return

    assignments.append("`updated_at` = NOW()")
    params.append(conversation_id)
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"UPDATE `chat_session` SET {', '.join(assignments)} WHERE `conversation_id` = %s",
                params,
            )
        conn.commit()


def get_conversation_history_records(
    conversation_id: str,
    limit: int = MAX_HISTORY_MESSAGES,
) -> list[dict[str, Any]]:
    conversation_id = normalize_conversation_id(conversation_id)
    ensure_runtime_ready()
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT `id`, `role`, `content`, COALESCE(`display_content`, `content`) AS `display_content`
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


def get_conversation_history(conversation_id: str) -> list[dict[str, str]]:
    rows = get_conversation_history_records(conversation_id, MAX_HISTORY_MESSAGES)
    return [{"role": row["role"], "content": row["content"]} for row in rows]


def get_conversation_messages_for_ui(conversation_id: str) -> list[dict[str, str]]:
    rows = get_conversation_history_records(conversation_id, MAX_UI_HISTORY_MESSAGES)
    return [{"role": row["role"], "content": row["display_content"]} for row in rows]


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


def save_latest_result(conversation_id: str, result_payload: dict[str, Any]) -> None:
    conversation_id = normalize_conversation_id(conversation_id)
    ensure_chat_session(conversation_id)
    normalized_payload = normalize_latest_result(result_payload)
    payload_json = json.dumps(normalized_payload, ensure_ascii=False, default=str)
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


def normalize_context_stats(raw_stats: Any, llm_provider: str | None = None) -> dict[str, Any]:
    stats = safe_load_json_dict(raw_stats)
    provider_name = normalize_llm_provider(stats.get("llm_provider") or llm_provider) or DEFAULT_LLM_PROVIDER
    meta = get_llm_provider_meta(provider_name)
    max_input_tokens = int(stats.get("max_input_tokens") or meta["max_input_tokens"])
    estimated_prompt_tokens = max(0, int(stats.get("estimated_prompt_tokens") or 0))
    remaining_tokens = max(0, max_input_tokens - estimated_prompt_tokens)
    source_message_count = max(0, int(stats.get("source_message_count") or 0))
    source_token_estimate = max(0, int(stats.get("source_token_estimate") or 0))
    compressed_token_estimate = max(0, int(stats.get("compressed_token_estimate") or estimated_prompt_tokens))
    saved_tokens = max(0, source_token_estimate - compressed_token_estimate)
    compression_ratio = 0 if source_token_estimate <= 0 else round(saved_tokens / source_token_estimate, 4)
    remaining_ratio = 0 if max_input_tokens <= 0 else round(remaining_tokens / max_input_tokens, 4)
    return {
        "strategy_label": stats.get("strategy_label") or CONTEXT_STRATEGY_LABEL,
        "llm_provider": provider_name,
        "llm_provider_label": stats.get("llm_provider_label") or meta["label"],
        "model": stats.get("model") or meta["model"],
        "max_input_tokens": max_input_tokens,
        "estimated_prompt_tokens": estimated_prompt_tokens,
        "remaining_tokens": remaining_tokens,
        "remaining_ratio": remaining_ratio,
        "source_message_count": source_message_count,
        "recent_message_count": max(0, int(stats.get("recent_message_count") or 0)),
        "summarized_message_count": max(0, int(stats.get("summarized_message_count") or 0)),
        "summary_token_estimate": max(0, int(stats.get("summary_token_estimate") or 0)),
        "source_token_estimate": source_token_estimate,
        "compressed_token_estimate": compressed_token_estimate,
        "saved_tokens": saved_tokens,
        "compression_ratio": compression_ratio,
        "history_text_length": max(0, int(stats.get("history_text_length") or 0)),
        "summary_updated": bool(stats.get("summary_updated")),
    }


def normalize_latest_result(raw_payload: Any) -> dict[str, Any]:
    payload = safe_load_json_dict(raw_payload)
    provider_name = normalize_llm_provider(payload.get("llm_provider")) or DEFAULT_LLM_PROVIDER
    meta = get_llm_provider_meta(provider_name)
    normalized = dict(payload)
    normalized["conversation_id"] = normalize_conversation_id(normalized.get("conversation_id"))
    normalized["reply_type"] = normalized.get("reply_type") or "result"
    normalized["dimensions"] = normalize_name_list(normalized.get("dimensions", []))
    normalized["metrics"] = normalize_name_list(normalized.get("metrics", []))
    normalized["columns"] = [str(item) for item in normalized.get("columns", [])] if isinstance(normalized.get("columns"), list) else []
    normalized["rows"] = normalized.get("rows", []) if isinstance(normalized.get("rows"), list) else []
    normalized["row_count"] = int(normalized.get("row_count") or len(normalized["rows"]))
    normalized["generated_sql"] = str(normalized.get("generated_sql", "")).strip()
    normalized["metric_definition"] = str(normalized.get("metric_definition", "")).strip()
    normalized["metric_description"] = str(normalized.get("metric_description", "")).strip()
    normalized["chart_title"] = str(normalized.get("chart_title", "")).strip()
    normalized["chart_label_field"] = str(normalized.get("chart_label_field", "")).strip()
    normalized["chart_value_field"] = str(normalized.get("chart_value_field", "")).strip()
    normalized["assistant_message"] = str(normalized.get("assistant_message", "")).strip()
    normalized["time_dimension"] = str(normalized.get("time_dimension", "")).strip()
    normalized["time_granularity"] = normalize_time_granularity(normalized.get("time_granularity", "none"))
    normalized["time_range_start"] = str(normalized.get("time_range_start", "")).strip()
    normalized["time_range_end"] = str(normalized.get("time_range_end", "")).strip()
    normalized["llm_provider"] = provider_name
    normalized["llm_provider_label"] = payload.get("llm_provider_label") or meta["label"]
    normalized["model"] = payload.get("model") or meta["model"]
    normalized["context_stats"] = normalize_context_stats(payload.get("context_stats"), provider_name)
    return normalized


def get_latest_result(conversation_id: str) -> dict[str, Any] | None:
    conversation_id = normalize_conversation_id(conversation_id)
    row = get_chat_session_row(conversation_id)
    if not row or not row.get("latest_result_json"):
        return None
    normalized = normalize_latest_result(row["latest_result_json"])
    normalized["conversation_id"] = conversation_id
    if not normalized.get("context_stats"):
        normalized["context_stats"] = normalize_context_stats(row.get("context_stats_json"), normalized.get("llm_provider"))
    return normalized


def normalize_name_list(raw_value: Any) -> list[str]:
    if isinstance(raw_value, list):
        values = raw_value
    elif isinstance(raw_value, str) and raw_value.strip():
        values = re.split(r"[,，/、]+", raw_value)
    else:
        values = []

    normalized: list[str] = []
    for value in values:
        item = str(value).strip()
        if item and item not in normalized:
            normalized.append(item)
    return normalized


def normalize_time_granularity(raw_value: Any) -> str:
    value = str(raw_value or "").strip().lower()
    mapping = {
        "day": "day",
        "date": "day",
        "天": "day",
        "日": "day",
        "week": "week",
        "周": "week",
        "month": "month",
        "月": "month",
        "none": "none",
        "": "none",
    }
    return mapping.get(value, "none")


def summarize_history_with_llm(
    existing_summary: str,
    records: list[dict[str, Any]],
    llm_provider: str,
) -> str:
    if not records:
        return normalize_summary_text(existing_summary)

    llm_runtime = get_llm_runtime(llm_provider)
    client = llm_runtime["client"]
    compact_messages = []
    for record in records:
        role_name = "用户" if record.get("role") == "user" else "助手"
        compact_messages.append(f"{role_name}: {compact_text(record.get('content', ''), 220)}")

    system_prompt = (
        "你是 ChatBI 的上下文压缩器。"
        "请把已有摘要和新增对话压缩成供后续 SQL 生成使用的滚动摘要。"
        "只保留与业务查询相关的信息：已确认的指标、维度、过滤条件、排序口径、时间范围、澄清结论、未解决歧义。"
        "忽略寒暄、重复表述、详细 SQL、冗长结果。"
        f"输出最多 {MAX_CONTEXT_SUMMARY_LINES} 行，每行以 - 开头，纯文本，不要 Markdown 标题。"
    )
    user_prompt = (
        f"已有摘要:\n{existing_summary or '无'}\n\n"
        f"新增历史消息:\n{chr(10).join(compact_messages)}"
    )
    completion = client.chat.completions.create(
        model=llm_runtime["model"],
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
    )
    content = completion.choices[0].message.content or ""
    return normalize_summary_text(content)


def build_context_bundle(
    conversation_id: str,
    history_records: list[dict[str, Any]],
    llm_provider: str,
) -> dict[str, Any]:
    session_row = get_chat_session_row(conversation_id) or {}
    source_messages = [{"role": row["role"], "content": row["content"]} for row in history_records]
    source_message_count = len(source_messages)
    source_token_estimate = estimate_message_tokens(source_messages)
    summary_text = normalize_summary_text(session_row.get("context_summary") or "")
    last_compacted_message_id = int(session_row.get("last_compacted_message_id") or 0)
    summarized_message_count = int(session_row.get("summary_message_count") or 0)
    summary_updated = False

    needs_compression = (
        source_message_count > CONTEXT_COMPRESSION_TRIGGER_MESSAGES
        or source_token_estimate > CONTEXT_COMPRESSION_TRIGGER_TOKENS
        or bool(summary_text)
    )

    recent_records = history_records
    if needs_compression and len(history_records) > MAX_CONTEXT_RECENT_MESSAGES:
        recent_records = history_records[-MAX_CONTEXT_RECENT_MESSAGES:]
        older_records = history_records[:-MAX_CONTEXT_RECENT_MESSAGES]
        if older_records:
            boundary_message_id = int(older_records[-1]["id"])
            delta_records = [row for row in older_records if int(row["id"]) > last_compacted_message_id]
            if delta_records or not summary_text:
                try:
                    summary_text = summarize_history_with_llm(summary_text, delta_records or older_records, llm_provider)
                except Exception:  # noqa: BLE001
                    summary_text = build_fallback_summary(summary_text, delta_records or older_records)
                summarized_message_count = len(older_records)
                last_compacted_message_id = boundary_message_id
                summary_updated = True
                update_chat_session_context(
                    conversation_id,
                    context_summary=summary_text,
                    summary_message_count=summarized_message_count,
                    last_compacted_message_id=last_compacted_message_id,
                )

    history_sections: list[str] = []
    if summary_text:
        history_sections.append(f"历史摘要:\n{summary_text}")
    if recent_records:
        recent_messages = [{"role": row["role"], "content": row["content"]} for row in recent_records]
        history_sections.append(f"最近对话:\n{format_history_lines(recent_messages, MAX_CONTEXT_RECENT_MESSAGES)}")
    history_text = "\n\n".join(history_sections) if history_sections else "无历史对话"

    compressed_message_count = len(recent_records) + (1 if summary_text else 0)
    compressed_token_estimate = estimate_text_tokens(history_text)

    return {
        "history_text": history_text,
        "stats": {
            "strategy_label": CONTEXT_STRATEGY_LABEL,
            "llm_provider": normalize_llm_provider(llm_provider) or DEFAULT_LLM_PROVIDER,
            "source_message_count": source_message_count,
            "recent_message_count": len(recent_records),
            "summarized_message_count": summarized_message_count,
            "source_token_estimate": source_token_estimate,
            "summary_token_estimate": estimate_text_tokens(summary_text) if summary_text else 0,
            "compressed_token_estimate": compressed_token_estimate,
            "compressed_message_count": compressed_message_count,
            "history_text_length": len(history_text),
            "summary_updated": summary_updated,
        },
    }


def validate_and_normalize_sql(sql: str) -> str:
    normalized = sql.strip().rstrip(";")
    if not normalized:
        raise ValueError("模型未生成 SQL")

    lower_sql = normalized.lower()
    if not (lower_sql.startswith("select") or lower_sql.startswith("with")):
        raise ValueError("只允许执行 SELECT / WITH 查询语句")

    forbidden = [
        "insert",
        "update",
        "delete",
        "drop",
        "truncate",
        "alter",
        "create",
        "grant",
        "revoke",
        "replace",
    ]
    for token in forbidden:
        if re.search(rf"\b{token}\b", lower_sql):
            raise ValueError(f"检测到不安全关键词: {token}")

    if ";" in normalized:
        raise ValueError("只允许单条 SQL")

    cte_names = extract_cte_names(lower_sql)
    table_matches = re.findall(r"\b(?:from|join)\s+`?([a-zA-Z_][\w]*)`?", lower_sql)
    invalid_tables = [
        table_name
        for table_name in table_matches
        if table_name not in ALLOWED_BASE_TABLES and table_name not in cte_names
    ]
    if invalid_tables:
        raise ValueError(f"检测到未授权表: {', '.join(sorted(set(invalid_tables)))}")

    has_limit = bool(re.search(r"\blimit\s+\d+(\s*,\s*\d+)?\b", lower_sql))
    if not has_limit:
        normalized = f"{normalized} LIMIT {MAX_RESULT_ROWS}"

    return normalized


def generate_query_plan_by_llm(
    conversation_id: str,
    question: str,
    history_records: list[dict[str, Any]],
    llm_provider: str,
) -> dict[str, Any]:
    llm_runtime = get_llm_runtime(llm_provider)
    client = llm_runtime["client"]
    semantic_context = retrieve_semantic_context(
        question,
        [{"role": row["role"], "content": row["content"]} for row in history_records],
    )
    context_bundle = build_context_bundle(conversation_id, history_records, llm_provider)

    system_prompt = (
        "你是资深数据分析工程师和ChatBI语义层设计者。"
        "系统不会直接给你全量 schema，而是先给你候选业务指标、候选表和候选关联关系。"
        "你必须基于这些候选信息生成 SQL；如果候选信息不足以安全回答，必须先澄清，不允许臆造字段或关联关系。"
        f"今天日期是 {TODAY_STR}。"
        "请根据候选语义层、历史对话和当前问题，输出一个 JSON 对象，不要解释，不要 Markdown。"
        "JSON 允许包含以下字段："
        "action、assistant_message、metric_definition、metric_description、dimensions、metrics、sql、chart_title、chart_label_field、chart_value_field、time_dimension、time_granularity、time_range_start、time_range_end。"
        "必须满足："
        "1) action 只能是 query 或 clarify；"
        "2) 如果当前问题缺少关键口径，无法安全生成 SQL，必须返回 action=clarify，并在 assistant_message 中提出一个简洁明确的问题；"
        "3) 如果用户是在追问、补充条件、补充新的地区/品牌/门店/时间范围，必须结合历史对话理解，不要丢失上下文；"
        "4) metric_definition 是简洁的指标定义名称，例如：近30天销售金额；"
        "5) metric_description 是完整的业务口径描述，说明查询哪张表、过滤条件、聚合逻辑；"
        "6) dimensions 必须返回业务维度名称数组，不能返回数据库字段名，例如 [\"销售大区\", \"下单日期\"]；如果没有维度则返回 []；"
        "7) metrics 必须返回业务指标名称数组，不能返回数据库字段名，例如 [\"销售金额\", \"订单数\"]；"
        "8) sql 的 SELECT 输出列别名必须尽量使用和 dimensions、metrics 一致的中文业务名称，不能直接输出原始字段名；"
        "9) 只有用户明确提到按天/按周/按月/按品牌/按门店/按省份/按大区等维度时，才做 GROUP BY；"
        "10) 如果用户没有明确分组维度，则返回整体汇总指标，不要自行拆分；"
        "11) 如果用户提到排名、TOP、前100，但没有说明按什么指标排序，必须先澄清；"
        "12) 订单和销售分析默认优先使用 order_master；商品、品牌、品类分析优先使用 order_detail 联表 product_info；用户属性分析使用 user_info；门店、地区、组织分析使用 store_info；退款分析使用 refund_master 或 refund_detail；"
        "13) order_master.order_status 的可用值只有：待支付、已支付、已发货、已完成、部分退款、已退款、已取消；禁止使用 completed、paid、shipped 等英文状态值；"
        "14) 当用户问销售金额、销量、GMV而未指定状态时，默认纳入 已支付、已发货、已完成、部分退款；"
        "15) 如果按品牌、产品、品类、SKU等商品粒度统计销售金额，必须使用 order_detail.line_paid_amount 或 line_gross_amount，不允许直接 SUM(order_master.paid_amount)，避免金额重复放大；"
        "16) 禁止臆造字段名；只能使用候选业务字段、默认表达式和候选关联关系中真实出现过的字段名；"
        "17) order_master 不存在 order_date、pay_date、ship_date 这类虚拟日期列；涉及下单日期请使用 created_at 或 DATE(order_master.created_at)；"
        "18) sql 只能使用允许的 7 张表；"
        "19) sql 只能生成 SELECT 或 WITH 查询；"
        "20) 如果用户没有限制条数，请在 sql 中加 LIMIT 200；"
        "21) chart_label_field 与 chart_value_field 如可判断，应返回 SELECT 中对应的中文别名；若不适合图表则返回空字符串；"
        "22) 如果问题包含明确或隐含的时间范围，必须返回 time_granularity，枚举值只能是 none/day/week/month；"
        "23) 如果问题中的时间语义是近N天、某个日期区间、按天，则 time_granularity 返回 day，并尽量给出 YYYY-MM-DD 格式的 time_range_start / time_range_end；"
        "24) 如果问题中的时间语义是按月或某月到某月，则 time_granularity 返回 month，并尽量给出 YYYY-MM 格式的 time_range_start / time_range_end；"
        "25) 如果问题中的时间语义是按周或某周到某周，则 time_granularity 返回 week，并尽量给出 YYYY-Www 格式的 time_range_start / time_range_end；"
        "26) 如果没有时间概念，则 time_granularity 返回 none，time_dimension / time_range_start / time_range_end 置空；"
        "27) 如果用户是在上一轮基础上只调整时间范围，必须保留原来的指标、维度和其他筛选条件。"
    )

    history_text = context_bundle["history_text"]
    user_prompt = (
        f"{semantic_context['prompt_text']}\n\n"
        f"历史对话:\n{history_text}\n\n"
        f"当前用户问题: {question}"
    )
    prompt_token_estimate = estimate_text_tokens(system_prompt) + estimate_text_tokens(user_prompt) + 24
    context_stats = normalize_context_stats(
        {
            **context_bundle["stats"],
            "llm_provider": llm_runtime["provider"],
            "llm_provider_label": llm_runtime["label"],
            "model": llm_runtime["model"],
            "max_input_tokens": llm_runtime["max_input_tokens"],
            "estimated_prompt_tokens": prompt_token_estimate,
        },
        llm_runtime["provider"],
    )
    update_chat_session_context(conversation_id, context_stats=context_stats)

    completion = client.chat.completions.create(
        model=llm_runtime["model"],
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
    )

    content = completion.choices[0].message.content or ""
    payload = extract_json_payload(content)

    action = str(payload.get("action", "query")).strip().lower()
    assistant_message = str(payload.get("assistant_message", "")).strip()
    metric_definition = str(payload.get("metric_definition", "")).strip()
    metric_description = str(payload.get("metric_description", "")).strip()
    sql = str(payload.get("sql", "")).strip()
    dimensions = normalize_name_list(payload.get("dimensions", []))
    metrics = normalize_name_list(payload.get("metrics", []))
    chart_title = str(payload.get("chart_title", "")).strip()
    chart_label_field = str(payload.get("chart_label_field", "")).strip()
    chart_value_field = str(payload.get("chart_value_field", "")).strip()
    time_dimension = str(payload.get("time_dimension", "")).strip()
    time_granularity = normalize_time_granularity(payload.get("time_granularity", "none"))
    time_range_start = str(payload.get("time_range_start", "")).strip()
    time_range_end = str(payload.get("time_range_end", "")).strip()

    if action not in {"query", "clarify"}:
        raise ValueError("模型返回了无效 action")

    if action == "clarify":
        if not assistant_message:
            raise ValueError("模型需要澄清但未返回问题")
        return {
            "action": action,
            "assistant_message": assistant_message,
            "dimensions": [],
            "metrics": [],
            "chart_title": "",
            "chart_label_field": "",
            "chart_value_field": "",
            "time_dimension": "",
            "time_granularity": "none",
            "time_range_start": "",
            "time_range_end": "",
            "candidate_tables": semantic_context["candidate_tables"],
            "candidate_metrics": semantic_context["candidate_metrics"],
            "llm_provider": llm_runtime["provider"],
            "llm_provider_label": llm_runtime["label"],
            "model": llm_runtime["model"],
            "context_stats": context_stats,
        }

    if not metric_definition:
        raise ValueError("模型未返回指标定义")
    if not metric_description:
        raise ValueError("模型未返回指标描述")
    if not metrics:
        raise ValueError("模型未返回指标名称")
    if not sql:
        raise ValueError("模型未返回 SQL")

    return {
        "action": action,
        "assistant_message": assistant_message or f"已生成查询结果：{metric_definition}",
        "metric_definition": metric_definition,
        "metric_description": metric_description,
        "dimensions": dimensions,
        "metrics": metrics,
        "sql": sql,
        "chart_title": chart_title or metric_definition,
        "chart_label_field": chart_label_field,
        "chart_value_field": chart_value_field,
        "time_dimension": time_dimension,
        "time_granularity": time_granularity,
        "time_range_start": time_range_start,
        "time_range_end": time_range_end,
        "candidate_tables": semantic_context["candidate_tables"],
        "candidate_metrics": semantic_context["candidate_metrics"],
        "llm_provider": llm_runtime["provider"],
        "llm_provider_label": llm_runtime["label"],
        "model": llm_runtime["model"],
        "context_stats": context_stats,
    }


def repair_sql_by_llm(
    conversation_id: str,
    question: str,
    history_records: list[dict[str, Any]],
    failed_sql: str,
    error_message: str,
    llm_provider: str,
) -> str:
    llm_runtime = get_llm_runtime(llm_provider)
    client = llm_runtime["client"]
    semantic_context = retrieve_semantic_context(
        question,
        [{"role": row["role"], "content": row["content"]} for row in history_records],
    )
    history_text = build_context_bundle(conversation_id, history_records, llm_provider)["history_text"]
    system_prompt = (
        "你是资深 MySQL SQL 修复助手。"
        "我会给你候选语义层、历史对话、原问题、失败 SQL 和数据库报错。"
        "你只能基于这些信息修复 SQL，禁止新增候选语义层之外的表、字段和关联关系。"
        "只输出一个 JSON 对象，不要解释，不要 Markdown。"
        "JSON 结构只能是 {\"sql\": \"...\"}。"
        "修复要求："
        "1) 只生成 SELECT 或 WITH；"
        "2) 修复未知字段、错误日期字段、错误 join、错误聚合；"
        "3) 禁止使用 order_date、pay_date、ship_date 这类虚拟字段；"
        "4) 若涉及下单日期，只能使用 created_at 或 DATE(order_master.created_at)；"
        "5) 商品粒度销售金额必须使用 order_detail.line_paid_amount 或 line_gross_amount；"
        "6) 只做必要修改，保留原业务意图。"
    )
    user_prompt = (
        f"{semantic_context['prompt_text']}\n\n"
        f"历史对话:\n{history_text}\n\n"
        f"当前问题: {question}\n"
        f"失败 SQL:\n{failed_sql}\n\n"
        f"MySQL 报错:\n{error_message}\n"
    )
    completion = client.chat.completions.create(
        model=llm_runtime["model"],
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
    )
    content = completion.choices[0].message.content or ""
    payload = extract_json_payload(content)
    repaired_sql = str(payload.get("sql", "")).strip()
    if not repaired_sql:
        raise ValueError("模型未返回修复后的 SQL")
    return repaired_sql


def run_query(sql: str) -> tuple[list[str], list[dict[str, Any]]]:
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute("SET SESSION MAX_EXECUTION_TIME = %s", (QUERY_TIMEOUT_MS,))
            except Exception:  # noqa: BLE001
                pass
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description] if cursor.description else []
            return columns, rows


@app.get("/")
def index() -> str:
    return render_template(
        "index.html",
        default_llm_provider=DEFAULT_LLM_PROVIDER,
        llm_provider_options=LLM_PROVIDER_OPTIONS,
    )


@app.get("/admin/semantic")
def semantic_admin() -> str:
    return render_template("semantic_admin.html")


@app.get("/api/admin/semantic/bootstrap")
def semantic_admin_bootstrap_api():
    try:
        ensure_runtime_ready()
        payload = get_admin_bootstrap()
        payload["maintenance_guide"] = get_semantic_maintenance_guide()
        return jsonify(payload)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.post("/api/admin/semantic/<entity>/save")
def semantic_admin_save_api(entity: str):
    payload = request.get_json(silent=True) or {}
    try:
        ensure_runtime_ready()
        upsert_admin_entity(entity, payload)
        return jsonify({"ok": True})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.post("/api/admin/semantic/<entity>/delete")
def semantic_admin_delete_api(entity: str):
    payload = request.get_json(silent=True) or {}
    try:
        ensure_runtime_ready()
        delete_admin_entity(entity, payload)
        return jsonify({"ok": True})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.post("/api/admin/semantic/sync-schema")
def semantic_admin_sync_schema_api():
    try:
        ensure_runtime_ready()
        sync_semantic_schema()
        result = rebuild_admin_search(refresh_embeddings=False)
        return jsonify({"ok": True, "result": result})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.post("/api/admin/semantic/rebuild")
def semantic_admin_rebuild_api():
    payload = request.get_json(silent=True) or {}
    refresh_embeddings = bool(payload.get("refresh_embeddings"))
    try:
        ensure_runtime_ready()
        result = rebuild_admin_search(refresh_embeddings=refresh_embeddings)
        return jsonify({"ok": True, "result": result})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/conversation/<conversation_id>")
def conversation_api(conversation_id: str):
    try:
        normalized_conversation_id = normalize_conversation_id(conversation_id)
        messages = get_conversation_messages_for_ui(normalized_conversation_id)
        latest_result = get_latest_result(normalized_conversation_id)
        session_row = get_chat_session_row(normalized_conversation_id) or {}
        return jsonify(
            {
                "conversation_id": normalized_conversation_id,
                "messages": messages,
                "latest_result": latest_result,
                "context_stats": normalize_context_stats(
                    (latest_result or {}).get("context_stats") or session_row.get("context_stats_json"),
                    (latest_result or {}).get("llm_provider"),
                ),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.post("/api/query")
def query_api():
    payload = request.get_json(silent=True) or {}
    question = str(payload.get("question", "")).strip()
    conversation_id = normalize_conversation_id(payload.get("conversation_id"))
    llm_provider = normalize_llm_provider(payload.get("llm_provider")) or DEFAULT_LLM_PROVIDER

    if not question:
        return jsonify({"error": "请输入查询问题或选择新的时间范围后再次提问"}), 400

    try:
        ensure_chat_session(conversation_id, title=question[:80])
        history_records = list(get_conversation_history_records(conversation_id, MAX_CONTEXT_SOURCE_MESSAGES))
        llm_result = generate_query_plan_by_llm(
            conversation_id,
            question,
            history_records,
            llm_provider,
        )

        if llm_result["action"] == "clarify":
            append_conversation_message(conversation_id, "user", question)
            append_conversation_message(
                conversation_id,
                "assistant",
                llm_result["assistant_message"],
                llm_result["assistant_message"],
            )
            return jsonify(
                {
                    "conversation_id": conversation_id,
                    "reply_type": "clarify",
                    "assistant_message": llm_result["assistant_message"],
                    "llm_provider": llm_result["llm_provider"],
                    "llm_provider_label": llm_result["llm_provider_label"],
                    "model": llm_result["model"],
                    "context_stats": llm_result["context_stats"],
                }
            )

        sql = validate_and_normalize_sql(llm_result["sql"])
        try:
            columns, rows = run_query(sql)
        except Exception as query_exc:  # noqa: BLE001
            repaired_sql = repair_sql_by_llm(
                conversation_id,
                question,
                history_records,
                sql,
                str(query_exc),
                llm_result["llm_provider"],
            )
            sql = validate_and_normalize_sql(repaired_sql)
            columns, rows = run_query(sql)
        assistant_display = (
            f"{llm_result['assistant_message']}\n"
            f"指标定义：{llm_result['metric_definition']}\n"
            f"指标描述：{llm_result['metric_description']}"
        )
        assistant_context = (
            f"{llm_result['assistant_message']} "
            f"指标定义: {llm_result['metric_definition']}。"
            f"指标描述: {llm_result['metric_description']}。"
            f"维度: {', '.join(llm_result['dimensions']) or '无'}。"
            f"指标: {', '.join(llm_result['metrics'])}。"
            f"候选表: {', '.join(llm_result['candidate_tables'])}。"
            f"SQL: {sql}"
        )

        append_conversation_message(conversation_id, "user", question)
        append_conversation_message(
            conversation_id,
            "assistant",
            assistant_context,
            assistant_display,
        )

        result_payload = {
            "conversation_id": conversation_id,
            "reply_type": "result",
            "question": question,
            "assistant_message": llm_result["assistant_message"],
            "metric_definition": llm_result["metric_definition"],
            "metric_description": llm_result["metric_description"],
            "dimensions": llm_result["dimensions"],
            "metrics": llm_result["metrics"],
            "generated_sql": sql,
            "chart_title": llm_result["chart_title"],
            "chart_label_field": llm_result["chart_label_field"],
            "chart_value_field": llm_result["chart_value_field"],
            "time_dimension": llm_result["time_dimension"],
            "time_granularity": llm_result["time_granularity"],
            "time_range_start": llm_result["time_range_start"],
            "time_range_end": llm_result["time_range_end"],
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "llm_provider": llm_result["llm_provider"],
            "llm_provider_label": llm_result["llm_provider_label"],
            "model": llm_result["model"],
            "context_stats": llm_result["context_stats"],
        }
        update_chat_session_context(conversation_id, context_stats=result_payload["context_stats"])
        save_latest_result(conversation_id, result_payload)
        return jsonify(result_payload)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


if __name__ == "__main__":
    ensure_runtime_ready()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=True)
