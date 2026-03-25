from __future__ import annotations

import math
import re
from typing import Any

from chatbi.utils.question_utils import compact_whitespace

TOKEN_PATTERN = re.compile(r'[A-Za-z0-9_\u4e00-\u9fff]+')
SOURCE_TYPE_BOOST = {
    'metric': 1.25,
    'dimension': 1.18,
    'table': 1.08,
    'join': 1.04,
    'example': 1.02,
    'synonym': 1.01,
    'column': 1.0,
}


def _tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_PATTERN.findall(compact_whitespace(text or '')) if token.strip()]


def _overlap_score(question_tokens: set[str], doc_tokens: set[str]) -> float:
    if not question_tokens or not doc_tokens:
        return 0.0
    overlap = question_tokens.intersection(doc_tokens)
    if not overlap:
        return 0.0
    precision = len(overlap) / max(1, len(doc_tokens))
    recall = len(overlap) / max(1, len(question_tokens))
    return (precision * 0.35) + (recall * 0.65)


def rerank_semantic_docs(
    question: str,
    scored_docs: list[tuple[dict[str, Any], float]],
    *,
    carryover_context: dict[str, Any] | None = None,
    top_k: int = 18,
    top_n: int = 10,
) -> list[tuple[dict[str, Any], float]]:
    shortlist = scored_docs[:top_k]
    if not shortlist:
        return []

    question_tokens = set(_tokenize(question))
    carryover_metrics = {str(item).strip().lower() for item in (carryover_context or {}).get('metrics', []) if str(item).strip()}
    carryover_dimensions = {str(item).strip().lower() for item in (carryover_context or {}).get('dimensions', []) if str(item).strip()}

    reranked: list[tuple[dict[str, Any], float]] = []
    for doc, base_score in shortlist:
        payload = doc.get('payload') or {}
        searchable_parts = [
            str(doc.get('source_name') or ''),
            str(doc.get('search_text') or ''),
            str(payload.get('description') or ''),
            str(payload.get('definition_name') or ''),
            str(payload.get('business_name') or ''),
            ' '.join(str(item) for item in payload.get('keywords', []) if item),
        ]
        doc_tokens = set()
        for part in searchable_parts:
            doc_tokens.update(_tokenize(part))
        score = float(base_score)
        score += _overlap_score(question_tokens, doc_tokens) * 25
        source_name = str(doc.get('source_name') or '').strip().lower()
        if source_name and source_name in compact_whitespace(question).lower():
            score += 8
        if doc.get('source_type') == 'metric' and source_name in carryover_metrics:
            score += 4
        if doc.get('source_type') == 'dimension' and source_name in carryover_dimensions:
            score += 4
        score *= SOURCE_TYPE_BOOST.get(str(doc.get('source_type') or ''), 1.0)
        score += math.log2(max(2, int(doc.get('priority_score') or 0) + 2))
        reranked.append((doc, score))

    reranked.sort(key=lambda item: item[1], reverse=True)
    return reranked[:top_n]
