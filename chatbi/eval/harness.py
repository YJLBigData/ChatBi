from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from chatbi.config import DEFAULT_LLM_PROVIDER
from chatbi.eval.cases import EvalCase, build_eval_cases
from chatbi.repository.chat_repository import normalize_conversation_id
from chatbi.service.conversation_service import normalize_latest_result
from chatbi.service.query_service import handle_user_query
from chatbi.service.runtime_service import ensure_runtime_ready


SECURITY_RANK = {'S0': 0, 'S1': 1, 'S2': 2}
EVAL_NAME_ALIAS_MAP = {
    '订单总数': '订单数',
    'gmv': '销售金额',
    '营业额': '销售金额',
    '退货量': '退款单数',
}


@dataclass
class EvalResult:
    case_id: str
    question: str
    expected_metrics: list[str]
    expected_dimensions: list[str]
    expected_tables: list[str]
    expect_clarify: bool
    security_level: str
    domain: str
    status: str
    passed: bool
    reply_type: str
    provider: str
    model: str
    latency_ms: int
    actual_metrics: list[str]
    actual_dimensions: list[str]
    actual_tables: list[str]
    actual_sql: str
    row_count: int
    error_message: str
    note: str


@dataclass
class EvalSummary:
    total: int
    evaluated: int
    skipped: int
    clarify_count: int
    executable_count: int
    success_count: int
    failure_count: int
    clarify_rate: float
    executable_rate: float
    success_rate: float
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    provider_counts: dict[str, int]
    security_counts: dict[str, int]
    domain_counts: dict[str, int]


def _normalize_security_level(raw_value: str) -> str:
    value = str(raw_value or 'S0').strip().upper()
    return value if value in SECURITY_RANK else 'S0'


def _security_allowed(case: EvalCase, max_security_level: str) -> bool:
    return SECURITY_RANK[_normalize_security_level(case.security_level)] <= SECURITY_RANK[_normalize_security_level(max_security_level)]


def _expected_tables_met(actual_sql: str, expected_tables: list[str]) -> bool:
    if not expected_tables:
        return True
    normalized_sql = (actual_sql or '').lower()
    actual_hits = [table for table in expected_tables if table.lower() in normalized_sql]
    if actual_hits:
        return True
    all_known_tables = [
        'order_master',
        'order_detail',
        'user_info',
        'product_info',
        'store_info',
        'refund_master',
        'refund_detail',
        'inventory_stock',
    ]
    detected_tables = {table for table in all_known_tables if table.lower() in normalized_sql}
    expected_set = {table.strip() for table in expected_tables if table.strip()}
    if not detected_tables:
        return False
    return detected_tables.issubset(expected_set)


def _subset_met(expected: list[str], actual: list[str]) -> bool:
    if not expected:
        return True

    def normalize_name(value: str) -> str:
        normalized = str(value or '').strip().lower()
        return EVAL_NAME_ALIAS_MAP.get(normalized, normalized)

    expected_set = {normalize_name(item) for item in expected if str(item).strip()}
    actual_set = {normalize_name(item) for item in actual if str(item).strip()}
    return expected_set.issubset(actual_set)


def _escape_markdown_cell(value: object) -> str:
    text = str(value or '')
    text = text.replace('\\', '\\\\')
    text = text.replace('|', '\\|')
    text = text.replace('\n', ' ')
    text = text.replace('\r', ' ')
    return text


def _run_internal(case: EvalCase, provider: str, client_id: str) -> dict[str, Any]:
    conversation_id = normalize_conversation_id(f'eval_{case.case_id}_{provider}_{uuid4().hex[:8]}')
    return handle_user_query(
        question=case.question,
        conversation_id=conversation_id,
        llm_provider=provider,
        client_id=client_id,
    )


def _run_api(case: EvalCase, provider: str, client_id: str) -> dict[str, Any]:
    from app import app as flask_app

    conversation_id = normalize_conversation_id(f'eval_{case.case_id}_{provider}_{uuid4().hex[:8]}')
    with flask_app.test_client() as client:
        response = client.post(
            '/api/query',
            json={
                'question': case.question,
                'conversation_id': conversation_id,
                'llm_provider': provider,
                'client_id': client_id,
            },
        )
        payload = response.get_json(silent=True) or {}
        if response.status_code >= 400:
            raise ValueError(payload.get('error') or f'API 返回 {response.status_code}')
        return payload


def evaluate_case(case: EvalCase, *, provider: str, transport: str, client_id: str) -> EvalResult:
    started_at = time.perf_counter()
    actual_payload: dict[str, Any] = {}
    error_message = ''
    status = 'error'
    try:
        if transport == 'api':
            actual_payload = _run_api(case, provider, client_id)
        else:
            actual_payload = _run_internal(case, provider, client_id)
        status = 'ok'
    except Exception as exc:  # noqa: BLE001
        error_message = str(exc)
        actual_payload = {}
    latency_ms = int((time.perf_counter() - started_at) * 1000)

    normalized = normalize_latest_result(actual_payload) if actual_payload else {}
    reply_type = str(normalized.get('reply_type') or actual_payload.get('reply_type') or '').strip()
    actual_metrics = [str(item) for item in normalized.get('metrics', []) if str(item).strip()]
    actual_dimensions = [str(item) for item in normalized.get('dimensions', []) if str(item).strip()]
    actual_sql = str(normalized.get('sql') or normalized.get('generated_sql') or actual_payload.get('sql') or '').strip()
    provider_name = str(normalized.get('llm_provider') or actual_payload.get('llm_provider') or provider).strip() or provider
    model_name = str(normalized.get('model') or actual_payload.get('model') or '').strip()
    row_count = int(normalized.get('row_count') or actual_payload.get('row_count') or 0)

    passed = False
    if status == 'ok':
        if case.expect_clarify:
            passed = reply_type == 'clarify'
        else:
            passed = (
                reply_type == 'result'
                and _subset_met(list(case.expected_metrics), actual_metrics)
                and _subset_met(list(case.expected_dimensions), actual_dimensions)
                and _expected_tables_met(actual_sql, list(case.expected_tables))
            )
    return EvalResult(
        case_id=case.case_id,
        question=case.question,
        expected_metrics=list(case.expected_metrics),
        expected_dimensions=list(case.expected_dimensions),
        expected_tables=list(case.expected_tables),
        expect_clarify=case.expect_clarify,
        security_level=case.security_level,
        domain=case.domain,
        status=status,
        passed=passed,
        reply_type=reply_type,
        provider=provider_name,
        model=model_name,
        latency_ms=latency_ms,
        actual_metrics=actual_metrics,
        actual_dimensions=actual_dimensions,
        actual_tables=[table for table in list(case.expected_tables) if table.lower() in actual_sql.lower()],
        actual_sql=actual_sql,
        row_count=row_count,
        error_message=error_message,
        note=case.note,
    )


def summarize_results(results: list[EvalResult], skipped: int = 0) -> EvalSummary:
    evaluated = len(results)
    clarify_count = sum(1 for item in results if item.reply_type == 'clarify')
    executable_count = sum(1 for item in results if item.reply_type == 'result' and not item.error_message)
    success_count = sum(1 for item in results if item.passed)
    failure_count = evaluated - success_count
    latencies = [item.latency_ms for item in results if item.latency_ms >= 0]
    provider_counts = Counter(item.provider or 'unknown' for item in results)
    security_counts = Counter(item.security_level for item in results)
    domain_counts = Counter(item.domain for item in results)
    avg_latency = float(round(statistics.mean(latencies), 2)) if latencies else 0.0
    p50_latency = float(round(statistics.median(latencies), 2)) if latencies else 0.0
    p95_latency = float(round(_percentile(latencies, 95), 2)) if latencies else 0.0
    total = evaluated + skipped
    return EvalSummary(
        total=total,
        evaluated=evaluated,
        skipped=skipped,
        clarify_count=clarify_count,
        executable_count=executable_count,
        success_count=success_count,
        failure_count=failure_count,
        clarify_rate=round(clarify_count / evaluated, 4) if evaluated else 0.0,
        executable_rate=round(executable_count / evaluated, 4) if evaluated else 0.0,
        success_rate=round(success_count / evaluated, 4) if evaluated else 0.0,
        avg_latency_ms=avg_latency,
        p50_latency_ms=p50_latency,
        p95_latency_ms=p95_latency,
        provider_counts=dict(provider_counts),
        security_counts=dict(security_counts),
        domain_counts=dict(domain_counts),
    )


def _percentile(values: list[int], percentile: int) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    k = (len(ordered) - 1) * (percentile / 100)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(ordered[int(k)])
    d0 = ordered[f] * (c - k)
    d1 = ordered[c] * (k - f)
    return float(d0 + d1)


def render_markdown_report(*, summary: EvalSummary, results: list[EvalResult], provider: str, transport: str, output_path: Path) -> str:
    lines: list[str] = []
    lines.append('# ChatBI Eval Report')
    lines.append('')
    lines.append(f'- Run at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    lines.append(f'- Provider: {provider}')
    lines.append(f'- Transport: {transport}')
    lines.append(f'- Output: `{output_path}`')
    lines.append('')
    lines.append('## Summary')
    lines.append('')
    lines.append('| metric | value |')
    lines.append('| --- | ---: |')
    lines.append(f'| total | {summary.total} |')
    lines.append(f'| evaluated | {summary.evaluated} |')
    lines.append(f'| skipped | {summary.skipped} |')
    lines.append(f'| clarify_count | {summary.clarify_count} |')
    lines.append(f'| executable_count | {summary.executable_count} |')
    lines.append(f'| success_count | {summary.success_count} |')
    lines.append(f'| failure_count | {summary.failure_count} |')
    lines.append(f'| clarify_rate | {summary.clarify_rate:.2%} |')
    lines.append(f'| executable_rate | {summary.executable_rate:.2%} |')
    lines.append(f'| success_rate | {summary.success_rate:.2%} |')
    lines.append(f'| avg_latency_ms | {summary.avg_latency_ms:.2f} |')
    lines.append(f'| p50_latency_ms | {summary.p50_latency_ms:.2f} |')
    lines.append(f'| p95_latency_ms | {summary.p95_latency_ms:.2f} |')
    lines.append('')
    lines.append('## Provider')
    lines.append('')
    for key, value in sorted(summary.provider_counts.items(), key=lambda item: item[0]):
        lines.append(f'- {key}: {value}')
    lines.append('')
    lines.append('## Security')
    lines.append('')
    for key, value in sorted(summary.security_counts.items(), key=lambda item: item[0]):
        lines.append(f'- {key}: {value}')
    lines.append('')
    lines.append('## Domains')
    lines.append('')
    for key, value in sorted(summary.domain_counts.items(), key=lambda item: item[0]):
        lines.append(f'- {key}: {value}')
    lines.append('')
    lines.append('## Failures')
    lines.append('')
    failed = [item for item in results if not item.passed]
    if not failed:
        lines.append('- none')
    else:
        lines.append('| case_id | status | reply_type | latency_ms | question | error |')
        lines.append('| --- | --- | --- | ---: | --- | --- |')
        for item in failed[:20]:
            question = _escape_markdown_cell(item.question)
            error_message = _escape_markdown_cell(item.error_message) or '-'
            lines.append(
                f"| {item.case_id} | {item.status} | {item.reply_type or '-'} | {item.latency_ms} | {question or '-'} | {error_message} |"
            )
    lines.append('')
    lines.append('## Samples')
    lines.append('')
    lines.append('| case_id | passed | reply_type | metrics | dimensions | tables |')
    lines.append('| --- | --- | --- | --- | --- | --- |')
    for item in results[:30]:
        metrics = _escape_markdown_cell(', '.join(item.actual_metrics or item.expected_metrics) or '-')
        dimensions = _escape_markdown_cell(', '.join(item.actual_dimensions or item.expected_dimensions) or '-')
        tables = _escape_markdown_cell(', '.join(item.expected_tables) or '-')
        lines.append(
            f"| {item.case_id} | {str(item.passed).lower()} | {item.reply_type or '-'} | {metrics} | {dimensions} | {tables} |"
        )
    report_text = '\n'.join(lines)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report_text, encoding='utf-8')
    return report_text


def write_markdown_report(*, summary: EvalSummary, results: list[EvalResult], provider: str, transport: str, output_path: Path) -> str:
    return render_markdown_report(
        summary=summary,
        results=results,
        provider=provider,
        transport=transport,
        output_path=output_path,
    )


def run_evaluation(
    *,
    provider: str | None = None,
    transport: str = 'internal',
    max_security_level: str = 'S2',
    limit: int | None = None,
    client_id: str = 'eval_regression',
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    ensure_runtime_ready()
    resolved_provider = provider or DEFAULT_LLM_PROVIDER
    normalized_transport = str(transport or 'internal').strip().lower()
    if normalized_transport not in {'internal', 'api'}:
        raise ValueError('transport 只能是 internal 或 api')
    normalized_max_security = _normalize_security_level(max_security_level)
    cases = build_eval_cases()
    if limit is not None:
        cases = cases[: max(0, int(limit))]
    results: list[EvalResult] = []
    skipped = 0
    for case in cases:
        if not _security_allowed(case, normalized_max_security):
            skipped += 1
            continue
        result = evaluate_case(case, provider=resolved_provider, transport=normalized_transport, client_id=client_id)
        results.append(result)
    summary = summarize_results(results, skipped=skipped)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = Path(output_path) if output_path else Path('scripts') / 'eval_reports' / f'chatbi_eval_{timestamp}.md'
    report_text = render_markdown_report(
        summary=summary,
        results=results,
        provider=resolved_provider,
        transport=normalized_transport,
        output_path=output_file,
    )
    return {
        'summary': asdict(summary),
        'results': [asdict(item) for item in results],
        'report_path': str(output_file),
        'report_text': report_text,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Run ChatBI evaluation dataset regression harness.')
    parser.add_argument('--provider', default=DEFAULT_LLM_PROVIDER, help='LLM provider name, e.g. bailian or deepseek')
    parser.add_argument('--transport', default='internal', choices=['internal', 'api'], help='Use internal handler or Flask API')
    parser.add_argument('--max-security', default='S2', choices=['S0', 'S1', 'S2'], help='Skip cases above the selected security level')
    parser.add_argument('--limit', type=int, default=None, help='Optional limit on number of cases to run')
    parser.add_argument('--client-id', default='eval_regression', help='Client id written to logs and tasks')
    parser.add_argument('--output', default='', help='Markdown report file path')
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    result = run_evaluation(
        provider=args.provider,
        transport=args.transport,
        max_security_level=args.max_security,
        limit=args.limit,
        client_id=args.client_id,
        output_path=args.output or None,
    )
    print(json.dumps({'report_path': result['report_path'], 'summary': result['summary']}, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
