import json
import logging
import re
from datetime import datetime
from functools import lru_cache
from typing import Any
from uuid import uuid4

from chatbi.config import ALLOWED_BASE_TABLES, MAX_CONTEXT_SOURCE_MESSAGES, MAX_RESULT_ROWS, QUERY_TIMEOUT_MS
from chatbi.prompt.query_prompt import build_query_plan_prompts, build_sql_repair_prompts
from chatbi.repository.chat_repository import (
    append_conversation_message,
    ensure_chat_session,
    get_conversation_history_records,
    get_chat_session_row,
    infer_next_round_no_from_history,
    update_chat_session_context,
)
from chatbi.repository.db import get_db_conn
from chatbi.service.context_service import build_context_bundle, estimate_text_tokens, normalize_context_stats
from chatbi.service.conversation_service import normalize_name_list, normalize_time_granularity, save_latest_result
from chatbi.service.llm_service import (
    DEFAULT_PROVIDER,
    build_execution_plan,
    chat_completion,
    get_llm_provider_meta,
    local_rewrite,
    normalize_llm_provider,
)
from chatbi.service.security_service import build_security_prompt_note, classify_security_level, filter_knowledge_context_for_online
from chatbi.utils.question_utils import compact_whitespace, is_context_dependent_question
from semantic_layer import retrieve_semantic_context

logger = logging.getLogger(__name__)

LOCATION_LITERAL_SOURCES: dict[str, list[tuple[str, str]]] = {
    'receiver_province': [('order_master', 'receiver_province')],
    'province': [('user_info', 'province'), ('store_info', 'province')],
    'receiver_city': [('order_master', 'receiver_city')],
    'city': [('user_info', 'city'), ('store_info', 'city')],
}

LOCATION_SUFFIXES = [
    '维吾尔自治区',
    '壮族自治区',
    '回族自治区',
    '特别行政区',
    '自治区',
    '省',
    '市',
]
ORDER_STATUS_LITERAL_MAP = {
    'pending': '待支付',
    'unpaid': '待支付',
    'paid': '已支付',
    'shipped': '已发货',
    'fulfilled': '已完成',
    'completed': '已完成',
    'partial_refund': '部分退款',
    'partial refunded': '部分退款',
    'refunded': '已退款',
    'cancelled': '已取消',
    'canceled': '已取消',
}
DEFAULT_PAYING_ORDER_STATUSES = ('已支付', '已发货', '已完成', '部分退款')
ORDER_STATUS_PATTERN = re.compile(
    r"(?P<lhs>(?:\b\w+\.)?order_status)\s+IN\s*\((?P<values>[^)]*)\)|(?P<lhs_eq>(?:\b\w+\.)?order_status)\s*=\s*'(?P<value>[^']*)'",
    re.IGNORECASE,
)

QUESTION_PREFIXES = ('统计', '查询', '分析', '查看', '帮我', '请帮我', '请', '麻烦')
QUESTION_SUFFIXES = ('。', '.', '？', '?', '！', '!')
MYSQL_INTERVAL_DATE_PATTERN = re.compile(
    r"DATE\(\s*'(?P<date>\d{4}-\d{2}-\d{2})'\s*(?P<op>[+-])\s*INTERVAL\s+(?P<count>\d+)\s+(?P<unit>DAY|WEEK|MONTH|YEAR)\s*\)",
    re.IGNORECASE,
)
AS_DOUBLE_QUOTED_ALIAS_PATTERN = re.compile(r'\bAS\s+"(?P<alias>[^"\n]+)"', re.IGNORECASE)
QUALIFIED_DOUBLE_QUOTED_IDENTIFIER_PATTERN = re.compile(r'(?P<prefix>\b[a-zA-Z_][\w]*\.)"(?P<identifier>[^"\n]+)"')
ORDER_GROUP_DOUBLE_QUOTED_PATTERN = re.compile(
    r'(?P<clause>\b(?:ORDER\s+BY|GROUP\s+BY|PARTITION\s+BY)\s+)"(?P<identifier>[^"\n]+)"',
    re.IGNORECASE,
)
SQL_TAIL_CLAUSE_PATTERN = re.compile(r'\b(GROUP\s+BY|ORDER\s+BY|LIMIT)\b', re.IGNORECASE)
COLUMN_REF_PATTERN = re.compile(
    r'\b(order_master|order_detail|refund_master|refund_detail|store_info|user_info|product_info|inventory_stock)\.([a-zA-Z_][\w]*)\b',
    re.IGNORECASE,
)
SQL_ALIAS_PATTERN = re.compile(r'\bAS\s+[`"]?(?P<alias>[^`",\n]+)[`"]?', re.IGNORECASE)
INVENTORY_JOIN_ORDER_DETAIL_PATTERN = re.compile(r'\bJOIN\s+order_detail\b', re.IGNORECASE)
INVENTORY_TABLE_REF_PATTERN = re.compile(r'\border_detail\.', re.IGNORECASE)
INVENTORY_FROM_PATTERN = re.compile(
    r'\bFROM\s+inventory_stock(?:\s+(?:AS\s+)?(?P<alias>[a-zA-Z_][\w]*))?(?=\s+(?:JOIN|WHERE|GROUP|ORDER|LIMIT)\b|\s*$)',
    re.IGNORECASE,
)


def extract_json_payload(text: str) -> dict[str, Any]:
    content = text.strip()
    code_block = re.search(r'```(?:json)?\s*(\{.*\})\s*```', content, re.IGNORECASE | re.DOTALL)
    if code_block:
        content = code_block.group(1).strip()
    elif not content.startswith('{'):
        json_match = re.search(r'(\{.*\})', content, re.DOTALL)
        if json_match:
            content = json_match.group(1).strip()
    payload = json.loads(content)
    if not isinstance(payload, dict):
        raise ValueError('模型返回格式错误，未得到 JSON 对象')
    return payload


def extract_cte_names(sql: str) -> set[str]:
    return set(re.findall(r'(?:(?:with)|,)\s*([a-zA-Z_][\w]*)\s+as\s*\(', sql, re.IGNORECASE))


def normalize_location_literal(value: str) -> str:
    normalized = compact_whitespace(value)
    for suffix in LOCATION_SUFFIXES:
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break
    return normalized.strip()


@lru_cache(maxsize=32)
def get_distinct_dimension_values(table_name: str, column_name: str) -> tuple[str, ...]:
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT DISTINCT `{column_name}` AS value FROM `{table_name}` WHERE `{column_name}` IS NOT NULL AND `{column_name}` <> ''"
            )
            rows = cursor.fetchall()
    values = []
    for row in rows:
        value = str(row.get('value') or '').strip()
        if value and value not in values:
            values.append(value)
    return tuple(values)


def resolve_dimension_literal(column_name: str, value: str) -> str:
    sources = LOCATION_LITERAL_SOURCES.get(column_name.lower())
    if not sources:
        return value
    raw_value = str(value or '').strip()
    if not raw_value:
        return raw_value
    exact_candidates: list[str] = []
    normalized_target = normalize_location_literal(raw_value)
    normalized_candidates: list[str] = []
    for table_name, source_column in sources:
        for actual_value in get_distinct_dimension_values(table_name, source_column):
            if actual_value == raw_value and actual_value not in exact_candidates:
                exact_candidates.append(actual_value)
            if normalize_location_literal(actual_value) == normalized_target and actual_value not in normalized_candidates:
                normalized_candidates.append(actual_value)
    if exact_candidates:
        return exact_candidates[0]
    if len(normalized_candidates) == 1:
        return normalized_candidates[0]
    return raw_value


def normalize_sql_filter_values(sql: str) -> str:
    if not sql:
        return sql
    columns_pattern = '|'.join(re.escape(column_name) for column_name in LOCATION_LITERAL_SOURCES)
    in_pattern = re.compile(
        rf'(?P<lhs>(?:\b\w+\.)?(?P<column>{columns_pattern}))\s+IN\s*\((?P<values>[^)]*)\)',
        re.IGNORECASE,
    )
    eq_pattern = re.compile(
        rf'(?P<lhs>(?:\b\w+\.)?(?P<column>{columns_pattern}))\s*=\s*\'(?P<value>[^\']*)\'',
        re.IGNORECASE,
    )

    def replace_in(match: re.Match[str]) -> str:
        column_name = match.group('column')
        raw_values = re.findall(r"'([^']*)'", match.group('values'))
        if not raw_values:
            return match.group(0)
        normalized_values = [resolve_dimension_literal(column_name, item) for item in raw_values]
        formatted = ', '.join(f"'{item}'" for item in normalized_values)
        return f"{match.group('lhs')} IN ({formatted})"

    def replace_eq(match: re.Match[str]) -> str:
        column_name = match.group('column')
        normalized_value = resolve_dimension_literal(column_name, match.group('value'))
        return f"{match.group('lhs')} = '{normalized_value}'"

    normalized_sql = in_pattern.sub(replace_in, sql)
    normalized_sql = eq_pattern.sub(replace_eq, normalized_sql)
    if normalized_sql != sql:
        logger.info('sql filter literals normalized original=%s normalized=%s', sql[:800], normalized_sql[:800])
    return normalized_sql


def normalize_order_status_filter_values(sql: str) -> str:
    if not sql:
        return sql

    def map_value(raw_value: str) -> str:
        normalized = compact_whitespace(raw_value).lower()
        return ORDER_STATUS_LITERAL_MAP.get(normalized, raw_value)

    def replace(match: re.Match[str]) -> str:
        if match.group('lhs'):
            raw_values = re.findall(r"'([^']*)'", match.group('values') or '')
            mapped_values = [map_value(value) for value in raw_values]
            formatted = ', '.join(f"'{value}'" for value in mapped_values)
            return f"{match.group('lhs')} IN ({formatted})"
        mapped = map_value(match.group('value') or '')
        return f"{match.group('lhs_eq')} = '{mapped}'"

    normalized_sql = ORDER_STATUS_PATTERN.sub(replace, sql)
    if normalized_sql != sql:
        logger.info('sql order_status literals normalized original=%s normalized=%s', sql[:800], normalized_sql[:800])
    return normalized_sql


def normalize_mysql_date_interval_expressions(sql: str) -> str:
    def replace(match: re.Match[str]) -> str:
        fn = 'DATE_SUB' if match.group('op') == '-' else 'DATE_ADD'
        return f"{fn}('{match.group('date')}', INTERVAL {match.group('count')} {match.group('unit').upper()})"

    normalized_sql = MYSQL_INTERVAL_DATE_PATTERN.sub(replace, sql)
    if normalized_sql != sql:
        logger.info('sql interval syntax normalized original=%s normalized=%s', sql[:800], normalized_sql[:800])
    return normalized_sql


def normalize_mysql_identifier_quotes(sql: str) -> str:
    normalized_sql = AS_DOUBLE_QUOTED_ALIAS_PATTERN.sub(lambda match: f"AS `{match.group('alias')}`", sql)
    normalized_sql = QUALIFIED_DOUBLE_QUOTED_IDENTIFIER_PATTERN.sub(
        lambda match: f"{match.group('prefix')}`{match.group('identifier')}`",
        normalized_sql,
    )
    normalized_sql = ORDER_GROUP_DOUBLE_QUOTED_PATTERN.sub(
        lambda match: f"{match.group('clause')}`{match.group('identifier')}`",
        normalized_sql,
    )
    if normalized_sql != sql:
        logger.info('sql identifier quotes normalized original=%s normalized=%s', sql[:800], normalized_sql[:800])
    return normalized_sql


def normalize_sql_mysql_dialect(sql: str) -> str:
    normalized_sql = normalize_mysql_identifier_quotes(str(sql or '').strip())
    normalized_sql = normalize_mysql_date_interval_expressions(normalized_sql)
    normalized_sql = normalize_order_status_filter_values(normalized_sql)
    return normalized_sql


def question_has_explicit_order_status(question: str) -> bool:
    normalized_question = compact_whitespace(question or '')
    return any(status in normalized_question for status in ('待支付', '已支付', '已发货', '已完成', '部分退款', '已退款', '已取消'))


def apply_default_metric_filters(sql: str, question: str, selected_metrics: list[str]) -> str:
    normalized_sql = str(sql or '').strip()
    if '支付买家数' not in (selected_metrics or []) or question_has_explicit_order_status(question):
        return normalized_sql

    default_clause = "order_master.order_status IN ('已支付', '已发货', '已完成', '部分退款')"

    if ORDER_STATUS_PATTERN.search(normalized_sql):
        replaced_sql = ORDER_STATUS_PATTERN.sub(default_clause, normalized_sql)
        if replaced_sql != normalized_sql:
            logger.info('sql default pay_buyer_count order_status enforced sql=%s', replaced_sql[:800])
        return replaced_sql

    lower_sql = normalized_sql.lower()
    tail_match = SQL_TAIL_CLAUSE_PATTERN.search(normalized_sql)
    insert_at = tail_match.start() if tail_match else len(normalized_sql)
    has_where_clause = bool(re.search(r'\bwhere\b', lower_sql))
    head = normalized_sql[:insert_at].rstrip()
    tail = normalized_sql[insert_at:].lstrip()
    if has_where_clause:
        rewritten_sql = f"{head}\n  AND {default_clause}"
    else:
        rewritten_sql = f"{head}\nWHERE {default_clause}"
    if tail:
        rewritten_sql = f"{rewritten_sql}\n{tail}"
    logger.info('sql default pay_buyer_count order_status appended sql=%s', rewritten_sql[:800])
    return rewritten_sql


def is_inventory_question(question: str, selected_metrics: list[str]) -> bool:
    normalized_question = compact_whitespace(question or '')
    inventory_metrics = {'在库量', '可售库存', '在途库存', '库存金额', '缺货SKU数'}
    return (
        '库存' in normalized_question
        or '仓库' in normalized_question
        or any(metric in inventory_metrics for metric in (selected_metrics or []))
    )


def apply_inventory_query_guards(sql: str, question: str, selected_metrics: list[str]) -> str:
    normalized_sql = str(sql or '').strip()
    lower_sql = normalized_sql.lower()
    if 'inventory_stock' not in lower_sql or not is_inventory_question(question, selected_metrics):
        return normalized_sql

    rewritten_sql = normalized_sql
    if 'join order_detail' in lower_sql and 'join product_info' not in lower_sql:
        rewritten_sql = INVENTORY_JOIN_ORDER_DETAIL_PATTERN.sub('JOIN product_info', rewritten_sql)
        rewritten_sql = INVENTORY_TABLE_REF_PATTERN.sub('product_info.', rewritten_sql)
        logger.info('inventory sql join normalized sql=%s', rewritten_sql[:800])

    if 'snapshot_date' not in rewritten_sql.lower():
        from_match = INVENTORY_FROM_PATTERN.search(rewritten_sql)
        inventory_alias = (from_match.group('alias') if from_match and from_match.group('alias') else 'inventory_stock').strip()
        snapshot_clause = f"{inventory_alias}.snapshot_date = (SELECT MAX(snapshot_date) FROM inventory_stock)"
        tail_match = SQL_TAIL_CLAUSE_PATTERN.search(rewritten_sql)
        insert_at = tail_match.start() if tail_match else len(rewritten_sql)
        head = rewritten_sql[:insert_at].rstrip()
        tail = rewritten_sql[insert_at:].lstrip()
        has_where_clause = bool(re.search(r'\bwhere\b', rewritten_sql, re.IGNORECASE))
        if has_where_clause:
            rewritten_sql = f"{head}\n  AND {snapshot_clause}"
        else:
            rewritten_sql = f"{head}\nWHERE {snapshot_clause}"
        if tail:
            rewritten_sql = f"{rewritten_sql}\n{tail}"
        logger.info('inventory sql latest snapshot enforced sql=%s', rewritten_sql[:800])

    return rewritten_sql


def validate_and_normalize_sql(sql: str) -> str:
    normalized = normalize_sql_mysql_dialect(sql)
    if not normalized:
        raise ValueError('模型未生成 SQL')
    normalized = re.sub(r';+\s*$', '', normalized).strip()
    lower_sql = normalized.lower()
    if not (lower_sql.startswith('select') or lower_sql.startswith('with ')):
        raise ValueError('只允许 SELECT 或 WITH 查询')
    danger_keywords = ['insert ', 'update ', 'delete ', 'drop ', 'alter ', 'create ', 'truncate ', 'replace ']
    if any(keyword in lower_sql for keyword in danger_keywords):
        raise ValueError('检测到危险 SQL 关键字')
    if ';' in normalized:
        raise ValueError('只允许单条 SQL')
    cte_names = extract_cte_names(lower_sql)
    table_matches = re.findall(r'\b(?:from|join)\s+`?([a-zA-Z_][\w]*)`?', lower_sql)
    invalid_tables = [
        table_name
        for table_name in table_matches
        if table_name not in ALLOWED_BASE_TABLES and table_name not in cte_names
    ]
    if invalid_tables:
        raise ValueError(f"检测到未授权表: {', '.join(sorted(set(invalid_tables)))}")
    has_limit = bool(re.search(r'\blimit\s+\d+(\s*,\s*\d+)?\b', lower_sql))
    if not has_limit:
        normalized = f'{normalized} LIMIT {MAX_RESULT_ROWS}'
    return normalized


def extract_expression_columns(expression: str) -> set[str]:
    if not expression:
        return set()
    return {match.group(2).lower() for match in COLUMN_REF_PATTERN.finditer(str(expression))}


def find_matching_rules(
    selected_names: list[str],
    candidate_rules: list[dict[str, Any]],
    *,
    name_key: str,
) -> list[dict[str, Any]]:
    if not candidate_rules:
        return []
    if not selected_names:
        return candidate_rules

    normalized_selected = [compact_whitespace(name).lower() for name in selected_names if name]
    matched_rules: list[dict[str, Any]] = []
    for rule in candidate_rules:
        rule_name = compact_whitespace(str(rule.get(name_key, ''))).lower()
        if not rule_name:
            continue
        if any(rule_name == selected or rule_name in selected or selected in rule_name for selected in normalized_selected):
            matched_rules.append(rule)
    return matched_rules


def sanitize_question_for_definition(question: str) -> str:
    sanitized = compact_whitespace(question or '')
    for prefix in QUESTION_PREFIXES:
        if sanitized.startswith(prefix):
            sanitized = sanitized[len(prefix):].strip()
            break
    sanitized = sanitized.strip()
    while sanitized and sanitized[-1] in QUESTION_SUFFIXES:
        sanitized = sanitized[:-1].strip()
    return sanitized or '查询结果'


def build_metric_definition_fallback(question: str, metrics: list[str], dimensions: list[str]) -> str:
    sanitized_question = sanitize_question_for_definition(question)
    if metrics:
        metric_part = '、'.join(metrics)
        if dimensions:
            return f'{sanitized_question}（维度：{"、".join(dimensions)}；指标：{metric_part}）'
        return sanitized_question if any(metric in sanitized_question for metric in metrics) else f'{sanitized_question}：{metric_part}'
    return sanitized_question


def build_metric_description_fallback(
    *,
    candidate_tables: list[str],
    metrics: list[str],
    dimensions: list[str],
    question: str,
) -> str:
    table_part = '、'.join(candidate_tables) if candidate_tables else '候选业务表'
    metric_part = '、'.join(metrics) if metrics else '相关业务指标'
    dimension_part = '、'.join(dimensions) if dimensions else '整体汇总'
    return (
        f"基于 {table_part} 相关数据，结合当前问题“{sanitize_question_for_definition(question)}”，"
        f"按 {dimension_part} 统计 {metric_part}。如需精确口径，请以页面【生成 SQL】区域内容为准。"
    )


def maybe_rewrite_question_for_local(question: str, history_records: list[dict[str, Any]], llm_provider: str) -> str:
    llm_meta = get_llm_provider_meta(llm_provider)
    normalized_provider = normalize_llm_provider(llm_provider)
    if normalized_provider != 'local':
        return question
    if not is_context_dependent_question(question):
        return question
    history_lines = []
    for row in history_records[-4:]:
        role = '用户' if row.get('role') == 'user' else '助手'
        content = compact_whitespace(str(row.get('display_content') or row.get('content') or ''))
        if content:
            history_lines.append(f'{role}: {content[:180]}')
    rewrite_prompt = (
        '请把当前问题改写成更明确、可直接检索业务语义的一句话。'
        '如果当前问题已经足够清晰，直接原样返回。'
        f'\n最近对话:\n{chr(10).join(history_lines) or "无"}'
        f'\n当前问题:\n{question}'
    )
    try:
        rewritten = compact_whitespace(local_rewrite(rewrite_prompt))
    except Exception as exc:  # noqa: BLE001
        logger.warning('local rewrite skipped provider=%s error=%s', llm_provider, exc)
        return question
    if not rewritten:
        return question
    if len(rewritten) > max(len(question) * 2, 120):
        return question
    logger.info('local rewrite applied provider=%s original=%s rewritten=%s', llm_provider, question[:160], rewritten[:160])
    return rewritten


def extract_expression_column_refs(expression: str) -> tuple[set[str], set[str]]:
    qualified_refs: set[str] = set()
    bare_columns: set[str] = set()
    for table_name, column_name in COLUMN_REF_PATTERN.findall(str(expression or '')):
        qualified_refs.add(f'{table_name.lower()}.{column_name.lower()}')
        bare_columns.add(column_name.lower())
    return qualified_refs, bare_columns


def sql_mentions_semantic_columns(sql: str, expression: str) -> bool:
    qualified_refs, bare_columns = extract_expression_column_refs(expression)
    if not qualified_refs and not bare_columns:
        return True
    normalized_sql = str(sql or '').lower().replace('`', '')
    if any(ref in normalized_sql for ref in qualified_refs):
        return True
    return any(re.search(rf'\b{re.escape(column_name)}\b', normalized_sql) for column_name in bare_columns)


def extract_sql_aliases(sql: str) -> set[str]:
    aliases: set[str] = set()
    for match in SQL_ALIAS_PATTERN.finditer(str(sql or '')):
        alias = compact_whitespace(match.group('alias')).strip('`"').lower()
        if alias:
            aliases.add(alias)
    return aliases


def infer_rule_names_from_sql_and_question(
    sql: str,
    question: str,
    candidate_rules: list[dict[str, Any]] | None,
    *,
    name_key: str,
    expression_key: str,
) -> list[str]:
    rules = candidate_rules or []
    if not rules:
        return []

    normalized_question = compact_whitespace(question or '').lower()
    sql_aliases = extract_sql_aliases(sql)
    ranked: list[tuple[int, str]] = []
    for rule in rules:
        rule_name = compact_whitespace(str(rule.get(name_key, ''))).strip()
        if not rule_name:
            continue
        normalized_name = rule_name.lower()
        score = 0
        if normalized_name and normalized_name in normalized_question:
            score += 3
        if any(
            normalized_name == alias
            or normalized_name in alias
            or alias in normalized_name
            for alias in sql_aliases
        ):
            score += 4
        expression = str(rule.get(expression_key, '')).strip()
        if expression and sql_mentions_semantic_columns(sql, expression):
            score += 5
        if score > 0:
            ranked.append((score, rule_name))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    seen: set[str] = set()
    selected: list[str] = []
    for _, name in ranked:
        normalized = name.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        selected.append(name)
    return selected


def validate_semantic_alignment(
    sql: str,
    *,
    question: str,
    selected_dimensions: list[str],
    selected_metrics: list[str],
    candidate_group_dimension_rules: list[dict[str, Any]] | None = None,
    candidate_metric_rules: list[dict[str, Any]] | None = None,
) -> None:
    dimension_rules = find_matching_rules(
        selected_dimensions,
        candidate_group_dimension_rules or [],
        name_key='dimension_name',
    )
    if not dimension_rules and selected_dimensions:
        inferred_dimensions = infer_rule_names_from_sql_and_question(
            sql,
            question,
            candidate_group_dimension_rules or [],
            name_key='dimension_name',
            expression_key='source_expression',
        )
        dimension_rules = find_matching_rules(
            inferred_dimensions,
            candidate_group_dimension_rules or [],
            name_key='dimension_name',
        )
    metric_rules = find_matching_rules(
        selected_metrics,
        candidate_metric_rules or [],
        name_key='metric_name',
    )
    if not metric_rules and selected_metrics:
        inferred_metrics = infer_rule_names_from_sql_and_question(
            sql,
            question,
            candidate_metric_rules or [],
            name_key='metric_name',
            expression_key='default_expression',
        )
        metric_rules = find_matching_rules(
            inferred_metrics,
            candidate_metric_rules or [],
            name_key='metric_name',
        )
    issues: list[str] = []
    for rule in dimension_rules:
        dimension_name = str(rule.get('dimension_name', '')).strip()
        source_expression = str(rule.get('source_expression', '')).strip()
        if dimension_name and source_expression and not sql_mentions_semantic_columns(sql, source_expression):
            issues.append(f'维度“{dimension_name}”必须使用 {source_expression}')
    for rule in metric_rules:
        metric_name = str(rule.get('metric_name', '')).strip()
        default_expression = str(rule.get('default_expression', '')).strip()
        if metric_name and default_expression and not sql_mentions_semantic_columns(sql, default_expression):
            issues.append(f'指标“{metric_name}”必须命中 {default_expression}')
        if metric_name == '支付买家数':
            normalized_sql = str(sql or '').lower().replace('`', '')
            if 'order_status' not in normalized_sql and 'payment_status' not in normalized_sql:
                issues.append('指标“支付买家数”必须显式限定支付或履约订单状态')
            normalized_question = compact_whitespace(question or '')
            explicit_status = any(status in normalized_question for status in ('待支付', '已支付', '已发货', '已完成', '部分退款', '已退款', '已取消'))
            if not explicit_status and 'order_status' in normalized_sql:
                missing_statuses = [status for status in DEFAULT_PAYING_ORDER_STATUSES if status not in normalized_sql]
                if missing_statuses:
                    issues.append(f'指标“支付买家数”默认需要纳入 {",".join(DEFAULT_PAYING_ORDER_STATUSES)}')
    if issues:
        raise ValueError('业务语义校验失败：' + '；'.join(issues))


def run_query(sql: str) -> tuple[list[str], list[dict[str, Any]]]:
    logger.info('sql execute start length=%s', len(sql))
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute('SET SESSION MAX_EXECUTION_TIME = %s', (QUERY_TIMEOUT_MS,))
            except Exception:  # noqa: BLE001
                pass
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description] if cursor.description else []
            logger.info('sql execute success columns=%s rows=%s', len(columns), len(rows))
            return columns, rows


def generate_query_plan_by_llm(
    conversation_id: str,
    question: str,
    history_records: list[dict[str, Any]],
    llm_provider: str,
    *,
    client_id: str | None = None,
    request_id: str | None = None,
    round_no: int | None = None,
) -> dict[str, Any]:
    llm_provider = normalize_llm_provider(llm_provider) or DEFAULT_PROVIDER
    llm_meta = get_llm_provider_meta(llm_provider)
    semantic_question = maybe_rewrite_question_for_local(question, history_records, llm_provider)
    prior_result: dict[str, Any] | None = None
    if is_context_dependent_question(question):
        session_row = get_chat_session_row(conversation_id) or {}
        try:
            latest_result_json = session_row.get('latest_result_json')
            if latest_result_json:
                payload = json.loads(str(latest_result_json))
                if isinstance(payload, dict):
                    prior_result = payload
        except Exception:  # noqa: BLE001
            prior_result = None
    semantic_context = retrieve_semantic_context(
        semantic_question,
        [{'role': row['role'], 'content': row['content']} for row in history_records],
        carryover_context=prior_result,
        prompt_mode='query',
    )
    knowledge_context = semantic_context.get('knowledge_context') or {}
    security_info = classify_security_level(question, semantic_context, knowledge_context)
    execution_plan = build_execution_plan(llm_provider, 'query_plan', security_info['security_level'])
    effective_knowledge_context = knowledge_context
    if execution_plan.get('providers', [''])[0] != 'local':
        effective_knowledge_context = filter_knowledge_context_for_online(knowledge_context, security_info['security_level'])
    semantic_prompt_text = semantic_context.get('base_prompt_text') or semantic_context['prompt_text']
    if effective_knowledge_context.get('prompt_text'):
        semantic_prompt_text = f"{semantic_prompt_text}\n\n本地结构化知识层:\n{effective_knowledge_context['prompt_text']}"
    security_note = build_security_prompt_note(
        security_info['security_level'],
        security_info.get('security_reasons', []),
        execution_plan,
    )
    logger.info(
        'query plan start conversation_id=%s request_id=%s round_no=%s candidate_tables=%s candidate_metrics=%s security=%s',
        conversation_id,
        request_id or '',
        round_no or 0,
        ','.join(semantic_context['candidate_tables']),
        ','.join(semantic_context['candidate_metrics']),
        security_info['security_level'],
    )
    context_bundle = build_context_bundle(
        conversation_id,
        history_records,
        llm_provider,
        client_id=client_id,
        request_id=request_id,
        round_no=round_no,
    )
    system_prompt, user_prompt = build_query_plan_prompts(semantic_prompt_text, context_bundle['history_text'], question, security_note)
    prompt_token_estimate = estimate_text_tokens(system_prompt) + estimate_text_tokens(user_prompt) + 24
    context_stats = normalize_context_stats(
        {
            **context_bundle['stats'],
            'llm_provider': llm_meta['provider'],
            'llm_provider_label': llm_meta['label'],
            'model': llm_meta['model'],
            'max_input_tokens': llm_meta['max_input_tokens'],
            'estimated_prompt_tokens': prompt_token_estimate,
        },
        llm_meta['provider'],
    )
    update_chat_session_context(conversation_id, context_stats=context_stats)
    response = chat_completion(
        stage='query_plan',
        messages=[
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': user_prompt},
        ],
        provider_name=llm_provider,
        conversation_id=conversation_id,
        client_id=client_id,
        request_id=request_id,
        round_no=round_no,
        temperature=0,
        security_level=security_info['security_level'],
    )
    payload = extract_json_payload(response['content'])
    action = str(payload.get('action', 'query')).strip().lower()
    assistant_message = str(payload.get('assistant_message', '')).strip()
    metric_definition = str(payload.get('metric_definition', '')).strip()
    metric_description = str(payload.get('metric_description', '')).strip()
    sql = str(payload.get('sql', '')).strip()
    dimensions = normalize_name_list(payload.get('dimensions', []))
    metrics = normalize_name_list(payload.get('metrics', []))
    chart_title = str(payload.get('chart_title', '')).strip()
    chart_label_field = str(payload.get('chart_label_field', '')).strip()
    chart_value_field = str(payload.get('chart_value_field', '')).strip()
    time_dimension = str(payload.get('time_dimension', '')).strip()
    time_granularity = normalize_time_granularity(payload.get('time_granularity', 'none'))
    time_range_start = str(payload.get('time_range_start', '')).strip()
    time_range_end = str(payload.get('time_range_end', '')).strip()
    if action not in {'query', 'clarify'}:
        raise ValueError('模型返回了无效 action')
    if action == 'clarify':
        if not assistant_message:
            raise ValueError('模型需要澄清但未返回问题')
        return {
            'action': action,
            'assistant_message': assistant_message,
            'dimensions': normalize_name_list(semantic_context.get('candidate_dimensions', [])),
            'metrics': normalize_name_list(semantic_context.get('candidate_metrics', [])),
            'chart_title': '',
            'chart_label_field': '',
            'chart_value_field': '',
            'time_dimension': '',
            'time_granularity': 'none',
            'time_range_start': '',
            'time_range_end': '',
            'candidate_tables': semantic_context['candidate_tables'],
            'candidate_metrics': semantic_context['candidate_metrics'],
            'candidate_dimensions': semantic_context.get('candidate_dimensions', []),
            'candidate_group_dimension_rules': semantic_context.get('candidate_group_dimension_rules', []),
            'candidate_metric_rules': semantic_context.get('candidate_metric_rules', []),
            'candidate_dimension_rules': semantic_context.get('candidate_dimension_rules', []),
            'llm_provider': llm_meta['provider'],
            'llm_provider_label': llm_meta['label'],
            'actual_provider': response.get('actual_provider', llm_meta['provider']),
            'actual_label': response.get('actual_label', llm_meta['label']),
            'model': response['model'],
            'context_stats': context_stats,
            'security_level': security_info['security_level'],
            'security_reasons': security_info['security_reasons'],
            'execution_plan': execution_plan,
        }
    if not dimensions:
        dimensions = infer_rule_names_from_sql_and_question(
            sql,
            question,
            semantic_context.get('candidate_group_dimension_rules', []) or semantic_context.get('candidate_dimension_rules', []),
            name_key='dimension_name',
            expression_key='source_expression',
        )
    if not metrics:
        metrics = infer_rule_names_from_sql_and_question(
            sql,
            question,
            semantic_context.get('candidate_metric_rules', []),
            name_key='metric_name',
            expression_key='default_expression',
        )
    if not metrics:
        metrics = normalize_name_list(semantic_context.get('candidate_metrics', []))[:3]
    if not metric_definition:
        metric_definition = build_metric_definition_fallback(question, metrics, dimensions)
        logger.warning(
            'query plan fallback metric_definition conversation_id=%s request_id=%s round_no=%s question=%s',
            conversation_id,
            request_id or '',
            round_no or 0,
            question[:160],
        )
    if not metric_description:
        metric_description = build_metric_description_fallback(
            candidate_tables=semantic_context['candidate_tables'],
            metrics=metrics,
            dimensions=dimensions,
            question=question,
        )
        logger.warning(
            'query plan fallback metric_description conversation_id=%s request_id=%s round_no=%s question=%s',
            conversation_id,
            request_id or '',
            round_no or 0,
            question[:160],
        )
    if not metrics:
        raise ValueError('模型未返回指标名称')
    if not sql:
        raise ValueError('模型未返回 SQL')
    return {
        'action': action,
        'assistant_message': assistant_message or f'已生成查询结果：{metric_definition}',
        'metric_definition': metric_definition,
        'metric_description': metric_description,
        'dimensions': dimensions,
        'metrics': metrics,
        'sql': sql,
        'chart_title': chart_title or metric_definition,
        'chart_label_field': chart_label_field,
        'chart_value_field': chart_value_field,
        'time_dimension': time_dimension,
        'time_granularity': time_granularity,
        'time_range_start': time_range_start,
        'time_range_end': time_range_end,
        'candidate_tables': semantic_context['candidate_tables'],
        'candidate_metrics': semantic_context['candidate_metrics'],
        'candidate_dimensions': semantic_context.get('candidate_dimensions', []),
        'candidate_group_dimension_rules': semantic_context.get('candidate_group_dimension_rules', []),
        'candidate_metric_rules': semantic_context.get('candidate_metric_rules', []),
        'candidate_dimension_rules': semantic_context.get('candidate_dimension_rules', []),
        'llm_provider': llm_meta['provider'],
        'llm_provider_label': llm_meta['label'],
        'actual_provider': response.get('actual_provider', llm_meta['provider']),
        'actual_label': response.get('actual_label', llm_meta['label']),
        'model': response['model'],
        'context_stats': context_stats,
        'security_level': security_info['security_level'],
        'security_reasons': security_info['security_reasons'],
        'execution_plan': execution_plan,
    }


def repair_sql_by_llm(
    conversation_id: str,
    question: str,
    history_records: list[dict[str, Any]],
    failed_sql: str,
    error_message: str,
    llm_provider: str,
    *,
    client_id: str | None = None,
    request_id: str | None = None,
    round_no: int | None = None,
) -> str:
    semantic_context = retrieve_semantic_context(
        maybe_rewrite_question_for_local(question, history_records, llm_provider),
        [{'role': row['role'], 'content': row['content']} for row in history_records],
        carryover_context=None,
        prompt_mode='repair',
        extra_sql_text=failed_sql,
    )
    knowledge_context = semantic_context.get('knowledge_context') or {}
    security_info = classify_security_level(question, semantic_context, knowledge_context)
    execution_plan = build_execution_plan(llm_provider, 'sql_repair', security_info['security_level'])
    effective_knowledge_context = knowledge_context
    if execution_plan.get('providers', [''])[0] != 'local':
        effective_knowledge_context = filter_knowledge_context_for_online(knowledge_context, security_info['security_level'])
    semantic_prompt_text = semantic_context.get('base_repair_prompt_text') or semantic_context.get('repair_prompt_text') or semantic_context['prompt_text']
    if effective_knowledge_context.get('prompt_text'):
        semantic_prompt_text = f"{semantic_prompt_text}\n\n本地结构化知识层:\n{effective_knowledge_context['prompt_text']}"
    security_note = build_security_prompt_note(
        security_info['security_level'],
        security_info.get('security_reasons', []),
        execution_plan,
    )
    history_text = build_context_bundle(
        conversation_id,
        history_records,
        llm_provider,
        client_id=client_id,
        request_id=request_id,
        round_no=round_no,
    )['history_text']
    system_prompt, user_prompt = build_sql_repair_prompts(
        semantic_prompt_text,
        history_text,
        question,
        failed_sql,
        error_message,
        security_note,
    )
    response = chat_completion(
        stage='sql_repair',
        messages=[
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': user_prompt},
        ],
        provider_name=llm_provider,
        conversation_id=conversation_id,
        client_id=client_id,
        request_id=request_id,
        round_no=round_no,
        temperature=0,
        security_level=security_info['security_level'],
    )
    payload = extract_json_payload(response['content'])
    repaired_sql = str(payload.get('sql', '')).strip()
    if not repaired_sql:
        raise ValueError('模型未返回修复后的 SQL')
    logger.info(
        'sql repaired conversation_id=%s request_id=%s round_no=%s original_len=%s repaired_len=%s',
        conversation_id,
        request_id or '',
        round_no or 0,
        len(failed_sql),
        len(repaired_sql),
    )
    return repaired_sql


def handle_user_query(
    *,
    question: str,
    conversation_id: str,
    llm_provider: str,
    client_id: str | None = None,
) -> dict[str, Any]:
    ensure_chat_session(conversation_id, title=question[:80])
    history_records = list(get_conversation_history_records(conversation_id, MAX_CONTEXT_SOURCE_MESSAGES))
    round_no = infer_next_round_no_from_history(history_records)
    request_id = f'req_{uuid4().hex[:16]}'
    llm_result = generate_query_plan_by_llm(
        conversation_id,
        question,
        history_records,
        llm_provider,
        client_id=client_id,
        request_id=request_id,
        round_no=round_no,
    )
    logger.info(
        'handle query conversation_id=%s request_id=%s round_no=%s action=%s question=%s',
        conversation_id,
        request_id,
        round_no,
        llm_result['action'],
        question[:120],
    )
    if llm_result['action'] == 'clarify':
        append_conversation_message(conversation_id, 'user', question)
        append_conversation_message(conversation_id, 'assistant', llm_result['assistant_message'], llm_result['assistant_message'])
        clarify_payload = {
            'conversation_id': conversation_id,
            'reply_type': 'clarify',
            'assistant_message': llm_result['assistant_message'],
            'question': question,
            'dimensions': llm_result.get('dimensions', []),
            'metrics': llm_result.get('metrics', []),
            'candidate_tables': llm_result.get('candidate_tables', []),
            'candidate_metrics': llm_result.get('candidate_metrics', []),
            'candidate_dimensions': llm_result.get('candidate_dimensions', []),
            'candidate_group_dimension_rules': llm_result.get('candidate_group_dimension_rules', []),
            'candidate_metric_rules': llm_result.get('candidate_metric_rules', []),
            'candidate_dimension_rules': llm_result.get('candidate_dimension_rules', []),
            'llm_provider': llm_result['llm_provider'],
            'llm_provider_label': llm_result['llm_provider_label'],
            'actual_provider': llm_result.get('actual_provider', llm_result['llm_provider']),
            'actual_provider_label': llm_result.get('actual_label', llm_result['llm_provider_label']),
            'model': llm_result['model'],
            'context_stats': llm_result['context_stats'],
            'security_level': llm_result.get('security_level', 'S1'),
            'security_reasons': llm_result.get('security_reasons', []),
            'execution_plan': llm_result.get('execution_plan', {}),
        }
        save_latest_result(conversation_id, clarify_payload)
        return clarify_payload
    sql = normalize_sql_filter_values(validate_and_normalize_sql(llm_result['sql']))
    sql = apply_default_metric_filters(sql, question, llm_result.get('metrics', []))
    sql = apply_inventory_query_guards(sql, question, llm_result.get('metrics', []))
    try:
        validate_semantic_alignment(
            sql,
            question=question,
            selected_dimensions=llm_result.get('dimensions', []),
            selected_metrics=llm_result.get('metrics', []),
            candidate_group_dimension_rules=llm_result.get('candidate_group_dimension_rules', []),
            candidate_metric_rules=llm_result.get('candidate_metric_rules', []),
        )
        columns, rows = run_query(sql)
    except Exception as query_exc:  # noqa: BLE001
        logger.warning(
            'sql execute failed conversation_id=%s request_id=%s round_no=%s error=%s sql=%s',
            conversation_id,
            request_id,
            round_no,
            query_exc,
            sql[:1000],
        )
        repaired_sql = repair_sql_by_llm(
            conversation_id,
            question,
            history_records,
            sql,
            str(query_exc),
            llm_result['llm_provider'],
            client_id=client_id,
            request_id=request_id,
            round_no=round_no,
        )
        sql = normalize_sql_filter_values(validate_and_normalize_sql(repaired_sql))
        sql = apply_default_metric_filters(sql, question, llm_result.get('metrics', []))
        sql = apply_inventory_query_guards(sql, question, llm_result.get('metrics', []))
        validate_semantic_alignment(
            sql,
            question=question,
            selected_dimensions=llm_result.get('dimensions', []),
            selected_metrics=llm_result.get('metrics', []),
            candidate_group_dimension_rules=llm_result.get('candidate_group_dimension_rules', []),
            candidate_metric_rules=llm_result.get('candidate_metric_rules', []),
        )
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
        f"时间粒度: {llm_result['time_granularity']}。"
        f"时间范围: {llm_result['time_range_start'] or '空'} 至 {llm_result['time_range_end'] or '空'}。"
    )
    append_conversation_message(conversation_id, 'user', question)
    append_conversation_message(conversation_id, 'assistant', assistant_context, assistant_display)
    result_payload = {
        'conversation_id': conversation_id,
        'reply_type': 'result',
        'question': question,
        'asked_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'query_round_no': round_no,
        'assistant_message': llm_result['assistant_message'],
        'metric_definition': llm_result['metric_definition'],
        'metric_description': llm_result['metric_description'],
        'dimensions': llm_result['dimensions'],
        'metrics': llm_result['metrics'],
        'sql': sql,
        'generated_sql': sql,
        'chart_title': llm_result['chart_title'],
        'chart_label_field': llm_result['chart_label_field'],
        'chart_value_field': llm_result['chart_value_field'],
        'time_dimension': llm_result['time_dimension'],
        'time_granularity': llm_result['time_granularity'],
        'time_range_start': llm_result['time_range_start'],
        'time_range_end': llm_result['time_range_end'],
        'columns': columns,
        'rows': rows,
        'row_count': len(rows),
        'llm_provider': llm_result['llm_provider'],
        'llm_provider_label': llm_result['llm_provider_label'],
        'actual_provider': llm_result.get('actual_provider', llm_result['llm_provider']),
        'actual_provider_label': llm_result.get('actual_label', llm_result['llm_provider_label']),
        'model': llm_result['model'],
        'context_stats': llm_result['context_stats'],
        'security_level': llm_result.get('security_level', 'S1'),
        'security_reasons': llm_result.get('security_reasons', []),
        'execution_plan': llm_result.get('execution_plan', {}),
    }
    save_latest_result(conversation_id, result_payload)
    logger.info(
        'handle query completed conversation_id=%s request_id=%s round_no=%s row_count=%s',
        conversation_id,
        request_id,
        round_no,
        len(rows),
    )
    return result_payload
