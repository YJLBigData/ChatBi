import json
import re
from datetime import datetime
from typing import Any
from uuid import uuid4

from chatbi.config import ALLOWED_BASE_TABLES, MAX_CONTEXT_SOURCE_MESSAGES, MAX_RESULT_ROWS, QUERY_TIMEOUT_MS
from chatbi.prompt.query_prompt import build_query_plan_prompts, build_sql_repair_prompts
from chatbi.repository.chat_repository import (
    append_conversation_message,
    ensure_chat_session,
    get_conversation_history_records,
    infer_next_round_no_from_history,
    update_chat_session_context,
)
from chatbi.repository.db import get_db_conn
from chatbi.service.context_service import build_context_bundle, estimate_text_tokens, normalize_context_stats
from chatbi.service.conversation_service import normalize_name_list, normalize_time_granularity, save_latest_result
from chatbi.service.llm_service import chat_completion, get_llm_provider_meta, normalize_llm_provider, DEFAULT_PROVIDER
from semantic_layer import retrieve_semantic_context


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


def validate_and_normalize_sql(sql: str) -> str:
    normalized = str(sql or '').strip()
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


def run_query(sql: str) -> tuple[list[str], list[dict[str, Any]]]:
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute('SET SESSION MAX_EXECUTION_TIME = %s', (QUERY_TIMEOUT_MS,))
            except Exception:  # noqa: BLE001
                pass
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description] if cursor.description else []
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
    semantic_context = retrieve_semantic_context(
        question,
        [{'role': row['role'], 'content': row['content']} for row in history_records],
    )
    context_bundle = build_context_bundle(
        conversation_id,
        history_records,
        llm_provider,
        client_id=client_id,
        request_id=request_id,
        round_no=round_no,
    )
    system_prompt, user_prompt = build_query_plan_prompts(semantic_context['prompt_text'], context_bundle['history_text'], question)
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
            'dimensions': [],
            'metrics': [],
            'chart_title': '',
            'chart_label_field': '',
            'chart_value_field': '',
            'time_dimension': '',
            'time_granularity': 'none',
            'time_range_start': '',
            'time_range_end': '',
            'candidate_tables': semantic_context['candidate_tables'],
            'candidate_metrics': semantic_context['candidate_metrics'],
            'llm_provider': llm_meta['provider'],
            'llm_provider_label': llm_meta['label'],
            'model': llm_meta['model'],
            'context_stats': context_stats,
        }
    if not metric_definition:
        raise ValueError('模型未返回指标定义')
    if not metric_description:
        raise ValueError('模型未返回指标描述')
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
        'llm_provider': llm_meta['provider'],
        'llm_provider_label': llm_meta['label'],
        'model': llm_meta['model'],
        'context_stats': context_stats,
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
        question,
        [{'role': row['role'], 'content': row['content']} for row in history_records],
    )
    history_text = build_context_bundle(
        conversation_id,
        history_records,
        llm_provider,
        client_id=client_id,
        request_id=request_id,
        round_no=round_no,
    )['history_text']
    system_prompt, user_prompt = build_sql_repair_prompts(semantic_context['prompt_text'], history_text, question, failed_sql, error_message)
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
    )
    payload = extract_json_payload(response['content'])
    repaired_sql = str(payload.get('sql', '')).strip()
    if not repaired_sql:
        raise ValueError('模型未返回修复后的 SQL')
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
    if llm_result['action'] == 'clarify':
        append_conversation_message(conversation_id, 'user', question)
        append_conversation_message(conversation_id, 'assistant', llm_result['assistant_message'], llm_result['assistant_message'])
        return {
            'conversation_id': conversation_id,
            'reply_type': 'clarify',
            'assistant_message': llm_result['assistant_message'],
            'llm_provider': llm_result['llm_provider'],
            'llm_provider_label': llm_result['llm_provider_label'],
            'model': llm_result['model'],
            'context_stats': llm_result['context_stats'],
        }
    sql = validate_and_normalize_sql(llm_result['sql'])
    try:
        columns, rows = run_query(sql)
    except Exception as query_exc:  # noqa: BLE001
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
        'model': llm_result['model'],
        'context_stats': llm_result['context_stats'],
    }
    save_latest_result(conversation_id, result_payload)
    return result_payload
