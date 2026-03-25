from __future__ import annotations

from typing import Any

LOCAL_FIRST_STAGES = {'context_summary', 'security_classify', 'query_rewrite', 'clarify_draft'}
ONLINE_FIRST_STAGES = {'query_plan', 'report_generate', 'sql_repair'}

HYBRID_TO_ONLINE = {
    'hybrid_bailian': 'bailian',
    'hybrid_deepseek': 'deepseek',
    'local_bailian': 'bailian',
    'local_deepseek': 'deepseek',
}


def build_provider_execution_plan(requested_provider: str, stage: str, security_level: str) -> dict[str, Any]:
    requested = str(requested_provider or '').strip() or 'bailian'
    security = str(security_level or 'S1').upper()
    stage_name = str(stage or '').strip() or 'query_plan'

    if requested in {'ollama', 'local'}:
        return {
            'requested_provider': requested,
            'security_level': security,
            'stage': stage_name,
            'providers': ['local'],
            'strategy_label': '单引擎·本地模型',
            'reason': '用户显式选择本地模型',
        }

    if requested in {'bailian', 'deepseek'}:
        if security == 'S2':
            return {
                'requested_provider': requested,
                'security_level': security,
                'stage': stage_name,
                'providers': ['local'],
                'strategy_label': '安全强制·本地模型',
                'reason': 'S2 敏感知识仅允许本地处理',
            }
        return {
            'requested_provider': requested,
            'security_level': security,
            'stage': stage_name,
            'providers': [requested],
            'strategy_label': f'单引擎·{requested}',
            'reason': '用户显式选择单引擎',
        }

    if requested in HYBRID_TO_ONLINE:
        online_provider = HYBRID_TO_ONLINE[requested]
        if security == 'S2':
            providers = ['local']
            reason = 'S2 敏感知识仅允许本地处理'
        elif stage_name in ONLINE_FIRST_STAGES:
            providers = [online_provider, 'local']
            reason = '双引擎模式：在线高智力规划，本地兜底'
        elif stage_name in LOCAL_FIRST_STAGES:
            providers = ['local', online_provider]
            reason = '双引擎模式：本地优先处理敏感或修复类任务'
        else:
            providers = ['local', online_provider]
            reason = '双引擎模式：本地优先，在线兜底'
        return {
            'requested_provider': requested,
            'security_level': security,
            'stage': stage_name,
            'providers': providers,
            'strategy_label': f'双引擎·本地模型 + {online_provider}',
            'reason': reason,
        }

    return {
        'requested_provider': 'bailian',
        'security_level': security,
        'stage': stage_name,
        'providers': ['bailian'],
        'strategy_label': '单引擎·bailian',
        'reason': '回退到默认在线引擎',
    }
