import json
import math
import re
from typing import Any

from chatbi.config import (
    CONTEXT_COMPRESSION_TRIGGER_MESSAGES,
    CONTEXT_COMPRESSION_TRIGGER_TOKENS,
    CONTEXT_STRATEGY_LABEL,
    MAX_CONTEXT_RECENT_MESSAGES,
    MAX_CONTEXT_SOURCE_MESSAGES,
    MAX_CONTEXT_SUMMARY_LINES,
)
from chatbi.prompt.query_prompt import build_summary_prompts
from chatbi.repository.chat_repository import get_chat_session_row, update_chat_session_context
from chatbi.service.llm_service import get_llm_provider_meta, normalize_llm_provider, chat_completion, DEFAULT_PROVIDER


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
    content = str(text or '')
    chinese_char_count = len(re.findall(r'[\u4e00-\u9fff]', content))
    word_count = len(re.findall(r'[A-Za-z0-9_]+', content))
    punctuation_count = len(re.findall(r'[^\w\s\u4e00-\u9fff]', content))
    whitespace_bonus = max(1, len(content) // 80)
    return max(1, chinese_char_count + math.ceil(word_count * 1.3) + math.ceil(punctuation_count * 0.3) + whitespace_bonus)


def estimate_message_tokens(messages: list[dict[str, str]]) -> int:
    return sum(estimate_text_tokens(f"{message.get('role', '')}:{message.get('content', '')}") + 4 for message in messages)


def compact_text(text: str, limit: int = 160) -> str:
    normalized = re.sub(r'\s+', ' ', str(text or '')).strip()
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[: limit - 1].rstrip()}…"


def normalize_summary_text(text: str) -> str:
    lines: list[str] = []
    for raw_line in str(text or '').splitlines():
        cleaned = re.sub(r'^\s*[-*•\d\.\)\(]+\s*', '', raw_line).strip()
        if not cleaned:
            continue
        normalized = f"- {compact_text(cleaned, 140)}"
        if normalized not in lines:
            lines.append(normalized)
        if len(lines) >= MAX_CONTEXT_SUMMARY_LINES:
            break
    return '\n'.join(lines)


def format_history_lines(history: list[dict[str, str]], max_messages: int | None = None) -> str:
    if not history:
        return '无历史对话'
    formatted = []
    messages = history[-max_messages:] if max_messages else history
    for message in messages:
        role_name = '用户' if message['role'] == 'user' else '助手'
        formatted.append(f"{role_name}: {message['content']}")
    return '\n'.join(formatted)


def build_fallback_summary(existing_summary: str, records: list[dict[str, Any]]) -> str:
    base_lines = normalize_summary_text(existing_summary).splitlines() if existing_summary else []
    new_lines: list[str] = []
    for record in records[-MAX_CONTEXT_SUMMARY_LINES:]:
        role_name = '用户' if record.get('role') == 'user' else '助手'
        snippet = compact_text(record.get('content', ''), 120)
        if snippet:
            new_lines.append(f'- {role_name}: {snippet}')
    merged_lines: list[str] = []
    for line in base_lines + new_lines:
        if line and line not in merged_lines:
            merged_lines.append(line)
        if len(merged_lines) >= MAX_CONTEXT_SUMMARY_LINES:
            break
    return '\n'.join(merged_lines)


def normalize_context_stats(raw_stats: Any, llm_provider: str | None = None) -> dict[str, Any]:
    stats = safe_load_json_dict(raw_stats)
    provider_name = normalize_llm_provider(stats.get('llm_provider') or llm_provider) or DEFAULT_PROVIDER
    meta = get_llm_provider_meta(provider_name)
    max_input_tokens = int(stats.get('max_input_tokens') or meta['max_input_tokens'])
    estimated_prompt_tokens = max(0, int(stats.get('estimated_prompt_tokens') or 0))
    remaining_tokens = max(0, max_input_tokens - estimated_prompt_tokens)
    source_message_count = max(0, int(stats.get('source_message_count') or 0))
    source_token_estimate = max(0, int(stats.get('source_token_estimate') or 0))
    compressed_token_estimate = max(0, int(stats.get('compressed_token_estimate') or estimated_prompt_tokens))
    saved_tokens = max(0, source_token_estimate - compressed_token_estimate)
    compression_ratio = 0 if source_token_estimate <= 0 else round(saved_tokens / source_token_estimate, 4)
    remaining_ratio = 0 if max_input_tokens <= 0 else round(remaining_tokens / max_input_tokens, 4)
    return {
        'strategy_label': stats.get('strategy_label') or CONTEXT_STRATEGY_LABEL,
        'llm_provider': provider_name,
        'llm_provider_label': stats.get('llm_provider_label') or meta['label'],
        'model': stats.get('model') or meta['model'],
        'max_input_tokens': max_input_tokens,
        'estimated_prompt_tokens': estimated_prompt_tokens,
        'remaining_tokens': remaining_tokens,
        'remaining_ratio': remaining_ratio,
        'source_message_count': source_message_count,
        'recent_message_count': max(0, int(stats.get('recent_message_count') or 0)),
        'summarized_message_count': max(0, int(stats.get('summarized_message_count') or 0)),
        'summary_token_estimate': max(0, int(stats.get('summary_token_estimate') or 0)),
        'source_token_estimate': source_token_estimate,
        'compressed_token_estimate': compressed_token_estimate,
        'saved_tokens': saved_tokens,
        'compression_ratio': compression_ratio,
        'history_text_length': max(0, int(stats.get('history_text_length') or 0)),
        'summary_updated': bool(stats.get('summary_updated')),
    }


def summarize_history_with_llm(
    existing_summary: str,
    records: list[dict[str, Any]],
    llm_provider: str,
    client_id: str | None = None,
    conversation_id: str | None = None,
    request_id: str | None = None,
    round_no: int | None = None,
) -> str:
    delta_text = format_history_lines([
        {'role': row['role'], 'content': row['content']} for row in records
    ])
    system_prompt, user_prompt = build_summary_prompts(existing_summary, delta_text)
    try:
        response = chat_completion(
            stage='context_summary',
            messages=[
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': user_prompt},
            ],
            provider_name=llm_provider,
            conversation_id=conversation_id,
            client_id=client_id,
            request_id=request_id,
            round_no=round_no,
            temperature=0.1,
        )
        normalized = normalize_summary_text(response['content'])
        return normalized or build_fallback_summary(existing_summary, records)
    except Exception:  # noqa: BLE001
        return build_fallback_summary(existing_summary, records)


def build_context_bundle(
    conversation_id: str,
    history_records: list[dict[str, Any]],
    llm_provider: str,
    *,
    client_id: str | None = None,
    request_id: str | None = None,
    round_no: int | None = None,
) -> dict[str, Any]:
    session_row = get_chat_session_row(conversation_id) or {}
    existing_summary = normalize_summary_text(session_row.get('context_summary') or '')
    source_messages = [{'role': row['role'], 'content': row['content']} for row in history_records]
    source_token_estimate = estimate_message_tokens(source_messages)
    needs_compression = (
        len(history_records) > CONTEXT_COMPRESSION_TRIGGER_MESSAGES
        or source_token_estimate > CONTEXT_COMPRESSION_TRIGGER_TOKENS
    )

    summary_updated = False
    summary_text = existing_summary
    recent_records = history_records
    summarized_count = int(session_row.get('summary_message_count') or 0)
    last_compacted_message_id = session_row.get('last_compacted_message_id')

    if needs_compression and len(history_records) > MAX_CONTEXT_RECENT_MESSAGES:
        recent_records = history_records[-MAX_CONTEXT_RECENT_MESSAGES:]
        older_records = history_records[:-MAX_CONTEXT_RECENT_MESSAGES]
        delta_records = older_records
        if last_compacted_message_id:
            delta_records = [row for row in older_records if int(row.get('id') or 0) > int(last_compacted_message_id)]
        if delta_records:
            summary_text = summarize_history_with_llm(
                summary_text,
                delta_records or older_records,
                llm_provider,
                client_id=client_id,
                conversation_id=conversation_id,
                request_id=request_id,
                round_no=round_no,
            )
            summary_updated = True
            summarized_count = len(older_records)
            last_compacted_message_id = int(older_records[-1]['id']) if older_records else last_compacted_message_id
            update_chat_session_context(
                conversation_id,
                context_summary=summary_text,
                summary_message_count=summarized_count,
                last_compacted_message_id=last_compacted_message_id,
            )

    recent_messages = [{'role': row['role'], 'content': row['content']} for row in recent_records]
    history_sections: list[str] = []
    if summary_text:
        history_sections.append(f'历史摘要:\n{summary_text}')
    if recent_messages:
        history_sections.append(f'最近对话:\n{format_history_lines(recent_messages, MAX_CONTEXT_RECENT_MESSAGES)}')
    history_text = '\n\n'.join(history_sections) if history_sections else '无历史对话'
    compressed_token_estimate = estimate_text_tokens(history_text)
    summary_token_estimate = estimate_text_tokens(summary_text) if summary_text else 0

    stats = normalize_context_stats(
        {
            'strategy_label': CONTEXT_STRATEGY_LABEL,
            'llm_provider': normalize_llm_provider(llm_provider) or DEFAULT_PROVIDER,
            'source_message_count': len(history_records),
            'recent_message_count': len(recent_records),
            'summarized_message_count': max(summarized_count, len(history_records) - len(recent_records)),
            'summary_token_estimate': summary_token_estimate,
            'source_token_estimate': source_token_estimate,
            'compressed_token_estimate': compressed_token_estimate,
            'history_text_length': len(history_text),
            'summary_updated': summary_updated,
        },
        llm_provider,
    )
    update_chat_session_context(conversation_id, context_stats=stats)
    return {
        'history_text': history_text,
        'summary_text': summary_text,
        'stats': stats,
        'session_row': session_row,
    }
