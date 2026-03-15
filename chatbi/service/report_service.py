import json
import re
from datetime import datetime
from typing import Any, Callable
from uuid import uuid4

from chatbi.config import MAX_CONTEXT_SOURCE_MESSAGES, REPORT_PREVIEW_MAX_ROWS
from chatbi.prompt.report_prompt import build_report_prompts
from chatbi.repository.chat_repository import get_chat_session_row, get_conversation_history_records, normalize_conversation_id
from chatbi.repository.db import get_db_conn
from chatbi.service.context_service import build_context_bundle
from chatbi.service.conversation_service import get_latest_result_or_raise
from chatbi.service.llm_service import chat_completion, get_llm_provider_meta
from reporting import (
    build_template_markdown_text,
    build_chart_word_bytes,
    build_csv_bytes,
    build_management_report_docx,
    get_report_template,
    save_report_history,
)


def sanitize_filename_component(text: str, fallback: str) -> str:
    cleaned = re.sub(r'[^\w\u4e00-\u9fff-]+', '_', str(text or '').strip())
    cleaned = cleaned.strip('_')
    return cleaned[:80] or fallback


def build_download_name(prefix: str, latest_result: dict[str, Any], suffix: str) -> str:
    metric_name = sanitize_filename_component(latest_result.get('metric_definition') or latest_result.get('chart_title'), 'chatbi')
    conversation_name = sanitize_filename_component(latest_result.get('conversation_id'), 'conversation')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f'{prefix}_{metric_name}_{conversation_name}_{timestamp}.{suffix}'


def build_report_download_name(latest_result: dict[str, Any]) -> str:
    metric_text = sanitize_filename_component(latest_result.get('metric_definition') or '商业分析报告', '商业分析报告')
    dimension_text = sanitize_filename_component('、'.join(latest_result.get('dimensions', [])) or '整体', '整体')
    raw_asked_at = str(latest_result.get('asked_at') or '').strip()
    ask_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    if raw_asked_at:
        normalized = re.sub(r'[^0-9]', '', raw_asked_at)
        if len(normalized) >= 14:
            ask_time = f'{normalized[:8]}_{normalized[8:14]}'
    return f'{metric_text}_{dimension_text}_{ask_time}.docx'


def build_report_task_display_name(latest_result: dict[str, Any]) -> str:
    metric_text = sanitize_filename_component(latest_result.get('metric_definition') or '商业报告', '商业报告')
    dimension_text = sanitize_filename_component('、'.join(latest_result.get('dimensions', [])) or '整体', '整体')
    raw_asked_at = str(latest_result.get('asked_at') or '').strip()
    ask_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    if raw_asked_at:
        normalized = re.sub(r'[^0-9]', '', raw_asked_at)
        if len(normalized) >= 14:
            ask_time = f'{normalized[:8]}_{normalized[8:14]}'
    return f'{metric_text}_{dimension_text}_{ask_time}'


def build_rows_preview(rows: list[dict[str, Any]], columns: list[str], max_rows: int = REPORT_PREVIEW_MAX_ROWS) -> str:
    preview_rows = []
    for row in rows[:max_rows]:
        preview_rows.append({column: row.get(column) for column in columns})
    return json.dumps(preview_rows, ensure_ascii=False)


def clamp_template_prompt_text(text: str, max_chars: int = 4000) -> str:
    normalized = str(text or '').strip()
    if len(normalized) <= max_chars:
        return normalized
    return f'{normalized[:max_chars].rstrip()}\n\n[模板提示词内容过长，已截断展示]'


def extract_json_payload(text: str) -> dict[str, Any]:
    content = str(text or '').strip()
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


def normalize_report_payload(payload: dict[str, Any], latest_result: dict[str, Any]) -> dict[str, Any]:
    def _string_list(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        result: list[str] = []
        for item in value:
            text = str(item).strip()
            if text:
                result.append(text)
        return result

    analysis_sections = []
    for item in payload.get('professional_analysis', []) if isinstance(payload.get('professional_analysis'), list) else []:
        if not isinstance(item, dict):
            continue
        title = str(item.get('title', '')).strip()
        content = str(item.get('content', '')).strip()
        if title or content:
            analysis_sections.append({'title': title or '专业分析', 'content': content})

    fallback_metric = latest_result.get('metric_definition', '经营指标分析')
    dimensions_text = '、'.join(latest_result.get('dimensions', [])) or '整体汇总'
    metrics_text = '、'.join(latest_result.get('metrics', [])) or '核心指标'
    rows = latest_result.get('rows', []) or []
    top_row_text = ''
    if rows:
        top_row = rows[0]
        top_row_text = '；'.join(f"{key}={top_row.get(key, '')}" for key in list(top_row.keys())[:4])

    return {
        'report_title': str(payload.get('report_title', '')).strip() or f'{fallback_metric}商业分析报告',
        'report_subtitle': str(payload.get('report_subtitle', '')).strip() or f'围绕 {dimensions_text} 对 {metrics_text} 进行经营复盘与策略分析',
        'executive_summary': str(payload.get('executive_summary', '')).strip() or f'本报告围绕 {fallback_metric} 展开，当前结果共返回 {latest_result.get("row_count", 0)} 行数据，建议管理层优先关注业务表现差异、结构变化与执行动作。',
        'management_summary': str(payload.get('management_summary', '')).strip() or f'从当前结果看，经营决策应围绕 {metrics_text} 与 {dimensions_text} 的结构性差异推进，重点关注头部表现与尾部改进空间。',
        'key_findings': _string_list(payload.get('key_findings')) or [f'当前分析围绕 {fallback_metric} 展开。', f'结果重点维度为 {dimensions_text}。', f'结果样本首行摘要：{top_row_text or "暂无样本行"}。'],
        'professional_analysis': analysis_sections or [{'title': '业务结构分析', 'content': f'当前分析主要围绕 {dimensions_text} 下的 {metrics_text} 变化展开，建议结合业务域和时间窗口继续跟踪结构差异。'}],
        'strategy_recommendations': _string_list(payload.get('strategy_recommendations')) or [f'围绕 {metrics_text} 建立分层经营策略。', f'针对 {dimensions_text} 维度识别高潜与低效单元，差异化配置资源。', '将关键指标纳入周/月度经营看板进行连续跟踪。'],
        'management_actions': _string_list(payload.get('management_actions')) or ['明确责任人和目标值。', '补充明细拆解并形成专项复盘。', '将改进动作纳入下一周期经营例会追踪。'],
        'risk_watchouts': _string_list(payload.get('risk_watchouts')) or ['样本窗口可能带来阶段性波动。', '部分异常变化需结合活动、渠道和库存背景复核。'],
        'appendix_note': str(payload.get('appendix_note', '')).strip() or '本报告由 ChatBI 自动生成，建议结合业务背景进行最终审阅。',
    }


def generate_report_content_by_llm(
    conversation_id: str,
    latest_result: dict[str, Any],
    llm_provider: str,
    template_row: dict[str, Any],
    *,
    client_id: str | None = None,
) -> dict[str, Any]:
    session_row = get_chat_session_row(conversation_id) or {}
    history_records = get_conversation_history_records(conversation_id, MAX_CONTEXT_SOURCE_MESSAGES)
    request_id = f'req_{uuid4().hex[:16]}'
    round_no = int(latest_result.get('query_round_no') or 0) or None
    context_bundle = build_context_bundle(
        conversation_id,
        history_records,
        llm_provider,
        client_id=client_id,
        request_id=request_id,
        round_no=round_no,
    )
    rows = latest_result.get('rows', []) or []
    columns = latest_result.get('columns', []) or []
    preview_text = build_rows_preview(rows, columns)
    context_text = session_row.get('context_summary') or context_bundle['history_text']
    system_prompt, user_prompt = build_report_prompts(
        latest_result,
        context_text,
        preview_text,
        clamp_template_prompt_text(build_template_markdown_text(template_row)),
    )
    response = chat_completion(
        stage='report_generate',
        messages=[
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': user_prompt},
        ],
        provider_name=llm_provider,
        conversation_id=conversation_id,
        client_id=client_id,
        request_id=request_id,
        round_no=round_no,
        temperature=0.2,
    )
    payload = extract_json_payload(response['content'])
    return normalize_report_payload(payload, latest_result)


def export_data_file(conversation_id: str) -> tuple[bytes, str]:
    latest_result = get_latest_result_or_raise(conversation_id)
    csv_bytes = build_csv_bytes(latest_result)
    download_name = build_download_name('chatbi_detail', latest_result, 'csv')
    return csv_bytes, download_name


def export_chart_word_file(conversation_id: str, template_id: str | None, chart_images: list[dict[str, Any]]) -> tuple[bytes, str]:
    latest_result = get_latest_result_or_raise(conversation_id)
    with get_db_conn() as conn:
        template_row = get_report_template(conn, template_id)
    document_bytes = build_chart_word_bytes(template_row, latest_result, chart_images)
    download_name = build_download_name('chart_snapshot', latest_result, 'docx')
    return document_bytes, download_name


def execute_report_generation_task(task_payload: dict[str, Any], progress: Callable[[int, dict[str, Any] | None], None] | None = None) -> dict[str, Any]:
    conversation_id = normalize_conversation_id(task_payload.get('conversation_id'))
    template_id = task_payload.get('template_id')
    llm_provider = task_payload.get('llm_provider')
    client_id = task_payload.get('client_id')
    chart_images = task_payload.get('chart_images') or []

    latest_result = get_latest_result_or_raise(conversation_id)
    provider_to_use = llm_provider or latest_result.get('llm_provider')
    with get_db_conn() as conn:
        template_row = get_report_template(conn, template_id)
    if progress:
        progress(20, {'step': '生成报告内容'})
    report_payload = generate_report_content_by_llm(
        conversation_id,
        latest_result,
        provider_to_use,
        template_row,
        client_id=client_id,
    )
    if progress:
        progress(60, {'step': '组装Word文档'})
    with get_db_conn() as conn:
        document_bytes = build_management_report_docx(template_row, latest_result, report_payload, chart_images)
        llm_meta = get_llm_provider_meta(provider_to_use)
        download_name = build_report_download_name(latest_result)
        report_history = save_report_history(
            conn,
            conversation_id=conversation_id,
            template_row=template_row,
            latest_result=latest_result,
            report_payload=report_payload,
            llm_provider=provider_to_use,
            model_name=llm_meta['model'],
            document_bytes=document_bytes,
            download_name=download_name,
        )
        conn.commit()
    if progress:
        progress(100, {'step': '完成'})
    return {
        'report_id': report_history['report_id'],
        'download_name': report_history['file_name'],
        'file_path': report_history['file_path'],
        'template_name': report_history['template_name'],
        'report_title': report_history['report_title'],
        'metric_definition': latest_result.get('metric_definition', ''),
        'dimensions': latest_result.get('dimensions', []),
    }
