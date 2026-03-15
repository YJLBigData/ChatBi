from typing import Any

from chatbi.repository.chat_repository import (
    ensure_chat_session,
    get_chat_session_row,
    get_conversation_messages_for_ui,
    normalize_conversation_id,
    save_latest_result_json,
)
from chatbi.service.context_service import normalize_context_stats, safe_load_json_dict
from chatbi.service.llm_service import DEFAULT_PROVIDER, get_llm_provider_meta, normalize_llm_provider


def normalize_name_list(raw_value: Any) -> list[str]:
    if not isinstance(raw_value, list):
        return []
    values: list[str] = []
    seen: set[str] = set()
    for item in raw_value:
        text = str(item or '').strip()
        if text and text not in seen:
            values.append(text)
            seen.add(text)
    return values


def normalize_time_granularity(raw_value: Any) -> str:
    text = str(raw_value or 'none').strip().lower()
    return text if text in {'none', 'day', 'week', 'month'} else 'none'


def normalize_latest_result(raw_payload: Any) -> dict[str, Any]:
    payload = safe_load_json_dict(raw_payload)
    provider_name = normalize_llm_provider(payload.get('llm_provider')) or DEFAULT_PROVIDER
    meta = get_llm_provider_meta(provider_name)
    normalized = dict(payload)
    normalized['conversation_id'] = normalize_conversation_id(normalized.get('conversation_id'))
    normalized['reply_type'] = normalized.get('reply_type') or 'result'
    normalized['dimensions'] = normalize_name_list(normalized.get('dimensions', []))
    normalized['metrics'] = normalize_name_list(normalized.get('metrics', []))
    normalized['columns'] = [str(item) for item in normalized.get('columns', [])] if isinstance(normalized.get('columns'), list) else []
    normalized['rows'] = normalized.get('rows', []) if isinstance(normalized.get('rows'), list) else []
    normalized['row_count'] = int(normalized.get('row_count') or len(normalized['rows']))
    normalized['query_round_no'] = int(normalized.get('query_round_no') or 0)
    normalized['generated_sql'] = str(normalized.get('generated_sql', '')).strip()
    normalized['metric_definition'] = str(normalized.get('metric_definition', '')).strip()
    normalized['metric_description'] = str(normalized.get('metric_description', '')).strip()
    normalized['chart_title'] = str(normalized.get('chart_title', '')).strip()
    normalized['chart_label_field'] = str(normalized.get('chart_label_field', '')).strip()
    normalized['chart_value_field'] = str(normalized.get('chart_value_field', '')).strip()
    normalized['assistant_message'] = str(normalized.get('assistant_message', '')).strip()
    normalized['time_dimension'] = str(normalized.get('time_dimension', '')).strip()
    normalized['time_granularity'] = normalize_time_granularity(normalized.get('time_granularity', 'none'))
    normalized['time_range_start'] = str(normalized.get('time_range_start', '')).strip()
    normalized['time_range_end'] = str(normalized.get('time_range_end', '')).strip()
    normalized['llm_provider'] = provider_name
    normalized['llm_provider_label'] = payload.get('llm_provider_label') or meta['label']
    normalized['model'] = payload.get('model') or meta['model']
    normalized['context_stats'] = normalize_context_stats(payload.get('context_stats'), provider_name)
    return normalized


def save_latest_result(conversation_id: str, result_payload: dict[str, Any]) -> None:
    ensure_chat_session(conversation_id)
    save_latest_result_json(conversation_id, normalize_latest_result(result_payload))


def get_latest_result(conversation_id: str) -> dict[str, Any] | None:
    normalized_conversation_id = normalize_conversation_id(conversation_id)
    row = get_chat_session_row(normalized_conversation_id)
    if not row or not row.get('latest_result_json'):
        return None
    normalized = normalize_latest_result(row['latest_result_json'])
    normalized['conversation_id'] = normalized_conversation_id
    if not normalized.get('context_stats'):
        normalized['context_stats'] = normalize_context_stats(row.get('context_stats_json'), normalized.get('llm_provider'))
    return normalized


def get_latest_result_or_raise(conversation_id: str) -> dict[str, Any]:
    latest_result = get_latest_result(conversation_id)
    if not latest_result:
        raise ValueError('当前会话还没有可导出的查询结果，请先执行查询')
    if latest_result.get('reply_type') != 'result':
        raise ValueError('当前会话最近一次返回不是查询结果，暂时无法导出')
    return latest_result


def get_conversation_view(conversation_id: str) -> dict[str, Any]:
    normalized_conversation_id = normalize_conversation_id(conversation_id)
    messages = get_conversation_messages_for_ui(normalized_conversation_id)
    latest_result = get_latest_result(normalized_conversation_id)
    session_row = get_chat_session_row(normalized_conversation_id) or {}
    return {
        'conversation_id': normalized_conversation_id,
        'messages': messages,
        'latest_result': latest_result,
        'context_stats': normalize_context_stats(
            (latest_result or {}).get('context_stats') or session_row.get('context_stats_json'),
            (latest_result or {}).get('llm_provider'),
        ),
    }
