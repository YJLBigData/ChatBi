from __future__ import annotations

from typing import Any

from chatbi.service.knowledge_service import compose_knowledge_prompt_text

SECURITY_LEVELS = ('S0', 'S1', 'S2')
SENSITIVE_TERMS = {
    'buyer_id', 'user_id', '手机号', 'mobile', '地址', 'address', '身份证', 'id_card',
    '明细导出', '原始明细', '用户明细', 'prompt', '提示词', 'schema', '字段字典', '表结构',
}
INTERNAL_TERMS = {
    'sql样例', '样例sql', 'sql示例', 'join', '关联关系', '字段释义', '业务解释', '知识库', '指标口径',
}


def classify_security_level(question: str, semantic_context: dict[str, Any] | None, knowledge_context: dict[str, Any] | None) -> dict[str, Any]:
    normalized_question = str(question or '').lower()
    candidate_tables = {str(item).strip() for item in (semantic_context or {}).get('candidate_tables', []) if str(item).strip()}
    matched_knowledge = knowledge_context or {}
    reasons: list[str] = []
    level = 'S0'

    if any(term in normalized_question for term in SENSITIVE_TERMS):
        level = 'S2'
        reasons.append('问题包含用户/结构化敏感词')
    elif any(term in normalized_question for term in INTERNAL_TERMS):
        level = 'S1'
        reasons.append('问题包含内部知识或 schema 语义词')

    if 'user_info' in candidate_tables:
        level = 'S1' if level == 'S0' else level
        reasons.append('命中用户域表 user_info')

    if matched_knowledge.get('max_security_level') == 'S2':
        level = 'S2'
        reasons.append('命中 S2 级知识')
    elif matched_knowledge.get('max_security_level') == 'S1' and level == 'S0':
        level = 'S1'
        reasons.append('命中 S1 级知识')

    return {
        'security_level': level,
        'security_reasons': reasons or ['默认公开聚合分析'],
    }


def filter_knowledge_context_for_online(knowledge_context: dict[str, Any], security_level: str) -> dict[str, Any]:
    if not knowledge_context:
        return {}
    level = str(security_level or 'S1').upper()
    filtered = dict(knowledge_context)
    if level in {'S1', 'S2'}:
        filtered['field_glossary'] = []
        filtered['sql_examples'] = []
        filtered['field_glossary_text'] = ''
        filtered['sql_examples_text'] = ''
    if level == 'S2':
        filtered['joins'] = []
        filtered['synonyms'] = []
    filtered['prompt_text'] = compose_knowledge_prompt_text(filtered)
    return filtered


def build_security_prompt_note(security_level: str, reasons: list[str], execution_plan: dict[str, Any]) -> str:
    return (
        f"安全等级：{security_level}。"
        f"判定原因：{'；'.join(reasons or ['无'])}。"
        f"执行策略：{execution_plan.get('strategy_label', '')}；"
        f"路由顺序：{' -> '.join(execution_plan.get('providers', [])) or '无'}。"
    )
