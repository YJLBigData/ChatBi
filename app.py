import json
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

LLM_PROVIDER_CONFIGS = {
    "bailian": {
        "label": "阿里百炼",
        "api_key": DASHSCOPE_API_KEY,
        "base_url": DASHSCOPE_BASE_URL,
        "model": DASHSCOPE_MODEL,
    },
    "deepseek": {
        "label": "DeepSeek",
        "api_key": DEEPSEEK_API_KEY,
        "base_url": DEEPSEEK_BASE_URL,
        "model": DEEPSEEK_MODEL,
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


def get_llm_runtime(provider_name: str | None = None) -> dict[str, Any]:
    resolved_provider = normalize_llm_provider(provider_name) or DEFAULT_LLM_PROVIDER
    config = LLM_PROVIDER_CONFIGS.get(resolved_provider)
    if not config:
        raise ValueError("不支持的模型引擎")
    if not config.get("api_key"):
        env_name = "DASHSCOPE_API_KEY" if resolved_provider == "bailian" else "DEEPSEEK_API_KEY"
        raise ValueError(f"缺少 {env_name}，请先在 .env 中配置")
    return {
        "provider": resolved_provider,
        "label": config["label"],
        "model": config["model"],
        "client": OpenAI(api_key=config["api_key"], base_url=config["base_url"]),
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


def ensure_runtime_ready() -> None:
    global RUNTIME_READY
    if RUNTIME_READY:
        return

    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(CHAT_SESSION_DDL)
            cursor.execute(CHAT_MESSAGE_DDL)
        conn.commit()
    ensure_semantic_runtime(refresh_embeddings=False)
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


def extract_cte_names(sql: str) -> set[str]:
    return set(re.findall(r"(?:(?:with)|,)\s*([a-zA-Z_][\w]*)\s+as\s*\(", sql, re.IGNORECASE))


def ensure_chat_session(conversation_id: str, title: str | None = None) -> None:
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


def get_conversation_history(conversation_id: str) -> list[dict[str, str]]:
    ensure_runtime_ready()
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT `role`, `content`
                FROM `chat_message`
                WHERE `conversation_id` = %s
                ORDER BY `id` DESC
                LIMIT %s
                """,
                (conversation_id, MAX_HISTORY_MESSAGES),
            )
            rows = list(cursor.fetchall())
    rows.reverse()
    return [{"role": row["role"], "content": row["content"]} for row in rows]


def get_conversation_messages_for_ui(conversation_id: str) -> list[dict[str, str]]:
    ensure_runtime_ready()
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT `role`, COALESCE(`display_content`, `content`) AS `display_content`
                FROM `chat_message`
                WHERE `conversation_id` = %s
                ORDER BY `id` DESC
                LIMIT %s
                """,
                (conversation_id, MAX_UI_HISTORY_MESSAGES),
            )
            rows = list(cursor.fetchall())
    rows.reverse()
    return [{"role": row["role"], "content": row["display_content"]} for row in rows]


def append_conversation_message(
    conversation_id: str,
    role: str,
    content: str,
    display_content: str | None = None,
) -> None:
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


def get_latest_result(conversation_id: str) -> dict[str, Any] | None:
    ensure_runtime_ready()
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT `latest_result_json` FROM `chat_session` WHERE `conversation_id` = %s",
                (conversation_id,),
            )
            row = cursor.fetchone()
    if not row or not row.get("latest_result_json"):
        return None
    return json.loads(row["latest_result_json"])


def format_conversation_history(history: list[dict[str, str]]) -> str:
    if not history:
        return "无历史对话"
    formatted = []
    for message in history[-MAX_HISTORY_MESSAGES:]:
        role_name = "用户" if message["role"] == "user" else "助手"
        formatted.append(f"{role_name}: {message['content']}")
    return "\n".join(formatted)


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
    question: str,
    history: list[dict[str, str]],
    llm_provider: str,
) -> dict[str, Any]:
    llm_runtime = get_llm_runtime(llm_provider)
    client = llm_runtime["client"]
    semantic_context = retrieve_semantic_context(question, history)

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

    history_text = format_conversation_history(history)
    user_prompt = (
        f"{semantic_context['prompt_text']}\n\n"
        f"历史对话:\n{history_text}\n\n"
        f"当前用户问题: {question}"
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
    }


def repair_sql_by_llm(
    question: str,
    history: list[dict[str, str]],
    failed_sql: str,
    error_message: str,
    llm_provider: str,
) -> str:
    llm_runtime = get_llm_runtime(llm_provider)
    client = llm_runtime["client"]
    semantic_context = retrieve_semantic_context(question, history)
    history_text = format_conversation_history(history)
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
        messages = get_conversation_messages_for_ui(conversation_id)
        latest_result = get_latest_result(conversation_id)
        return jsonify(
            {
                "conversation_id": conversation_id,
                "messages": messages,
                "latest_result": latest_result,
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.post("/api/query")
def query_api():
    payload = request.get_json(silent=True) or {}
    question = str(payload.get("question", "")).strip()
    conversation_id = str(payload.get("conversation_id", "")).strip() or "default"
    llm_provider = normalize_llm_provider(payload.get("llm_provider")) or DEFAULT_LLM_PROVIDER

    if not question:
        return jsonify({"error": "请输入查询问题或选择新的时间范围后再次提问"}), 400

    try:
        ensure_chat_session(conversation_id, title=question[:80])
        history = list(get_conversation_history(conversation_id))
        llm_result = generate_query_plan_by_llm(question, history, llm_provider)

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
                }
            )

        sql = validate_and_normalize_sql(llm_result["sql"])
        try:
            columns, rows = run_query(sql)
        except Exception as query_exc:  # noqa: BLE001
            repaired_sql = repair_sql_by_llm(
                question,
                history,
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
        }
        save_latest_result(conversation_id, result_payload)
        return jsonify(result_payload)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


if __name__ == "__main__":
    ensure_runtime_ready()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=True)
