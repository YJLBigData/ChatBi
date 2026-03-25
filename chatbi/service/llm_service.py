import logging
from typing import Any

from openai import OpenAI

from agent.sql_copilot import build_provider_execution_plan
from chatbi.config import (
    DEFAULT_LLM_PROVIDER,
    LLM_PROVIDER_ALIASES,
    LLM_PROVIDER_CONFIGS,
    LLM_REQUEST_TIMEOUT_SECONDS,
    OLLAMA_REQUEST_TIMEOUT_SECONDS,
)
from chatbi.llm.ollama_client import OllamaClient
from chatbi.repository.task_repository import insert_llm_invocation_log

logger = logging.getLogger(__name__)


def normalize_llm_provider(raw_value: Any) -> str:
    value = str(raw_value or '').strip().lower()
    return LLM_PROVIDER_ALIASES.get(value, '')


def _provider_available(provider_name: str) -> bool:
    config = LLM_PROVIDER_CONFIGS.get(provider_name) or {}
    if provider_name == 'local':
        return bool(config.get('base_url')) and bool(config.get('model'))
    return bool(config.get('api_key'))


def resolve_default_llm_provider() -> str:
    configured = normalize_llm_provider(DEFAULT_LLM_PROVIDER) or 'bailian'
    if _provider_available(configured):
        return configured
    for provider_name in ['bailian', 'deepseek', 'local']:
        if _provider_available(provider_name):
            return provider_name
    return configured


DEFAULT_PROVIDER = resolve_default_llm_provider()


def get_llm_provider_meta(provider_name: str | None = None) -> dict[str, Any]:
    resolved_provider = normalize_llm_provider(provider_name) or DEFAULT_PROVIDER
    config = LLM_PROVIDER_CONFIGS.get(resolved_provider) or LLM_PROVIDER_CONFIGS[DEFAULT_PROVIDER]
    return {
        'provider': resolved_provider,
        'label': config['label'],
        'model': config['model'],
        'max_input_tokens': config['max_input_tokens'],
        'mode': config.get('mode', 'single'),
        'online_provider': config.get('online_provider'),
        'local_provider': config.get('local_provider'),
    }


def build_execution_plan(provider_name: str | None, stage: str, security_level: str) -> dict[str, Any]:
    requested_provider = normalize_llm_provider(provider_name) or DEFAULT_PROVIDER
    return build_provider_execution_plan(requested_provider, stage, security_level)


def _get_online_runtime(provider_name: str) -> dict[str, Any]:
    config = LLM_PROVIDER_CONFIGS.get(provider_name)
    if not config:
        raise ValueError('不支持的在线模型引擎')
    if not config.get('api_key'):
        env_name = 'DASHSCOPE_API_KEY' if provider_name == 'bailian' else 'DEEPSEEK_API_KEY'
        raise ValueError(f'缺少 {env_name}，请先在 .env 中配置')
    return {
        'provider': provider_name,
        'label': config['label'],
        'model': config['model'],
        'max_input_tokens': config['max_input_tokens'],
        'client': OpenAI(
            api_key=config['api_key'],
            base_url=config['base_url'],
            timeout=LLM_REQUEST_TIMEOUT_SECONDS,
            max_retries=1,
        ),
    }


def _get_local_runtime() -> dict[str, Any]:
    config = LLM_PROVIDER_CONFIGS['local']
    return {
        'provider': 'local',
        'label': config['label'],
        'model': config['model'],
        'max_input_tokens': config['max_input_tokens'],
        'client': OllamaClient(
            base_url=config['base_url'],
            model=config['model'],
            timeout=OLLAMA_REQUEST_TIMEOUT_SECONDS,
        ),
    }


def _get_local_client() -> OllamaClient:
    return _get_local_runtime()['client']


def _invoke_provider(runtime: dict[str, Any], messages: list[dict[str, str]], temperature: float) -> str:
    if runtime['provider'] == 'local':
        return runtime['client'].chat(messages, temperature=temperature)
    completion = runtime['client'].chat.completions.create(
        model=runtime['model'],
        messages=messages,
        temperature=temperature,
    )
    return completion.choices[0].message.content or ''


def local_rewrite(prompt: str) -> str:
    return _get_local_client().rewrite(prompt)


def local_classify(prompt: str) -> str:
    return _get_local_client().classify(prompt)


def local_clarify(prompt: str) -> str:
    return _get_local_client().clarify(prompt)


def chat_completion(
    *,
    stage: str,
    messages: list[dict[str, str]],
    provider_name: str | None,
    conversation_id: str | None = None,
    client_id: str | None = None,
    request_id: str | None = None,
    round_no: int | None = None,
    temperature: float = 0,
    security_level: str = 'S1',
) -> dict[str, Any]:
    requested_provider = normalize_llm_provider(provider_name) or DEFAULT_PROVIDER
    requested_meta = get_llm_provider_meta(requested_provider)
    execution_plan = build_provider_execution_plan(requested_provider, stage, security_level)
    errors: list[str] = []

    for actual_provider in execution_plan['providers']:
        request_payload = {
            'requested_provider': requested_provider,
            'requested_label': requested_meta['label'],
            'resolved_provider': actual_provider,
            'stage': stage,
            'security_level': security_level,
            'execution_plan': execution_plan,
            'model': (LLM_PROVIDER_CONFIGS.get(actual_provider) or {}).get('model', ''),
            'messages': messages,
            'temperature': temperature,
        }
        try:
            runtime = _get_local_runtime() if actual_provider == 'local' else _get_online_runtime(actual_provider)
            logger.info(
                'llm request stage=%s requested=%s actual=%s model=%s security=%s conversation_id=%s request_id=%s round_no=%s',
                stage,
                requested_provider,
                runtime['provider'],
                runtime['model'],
                security_level,
                conversation_id or '',
                request_id or '',
                round_no or 0,
            )
            content = _invoke_provider(runtime, messages, temperature)
            response_payload = {
                'content': content,
                'requested_provider': requested_provider,
                'actual_provider': runtime['provider'],
                'security_level': security_level,
            }
            insert_llm_invocation_log(
                conversation_id=conversation_id,
                client_id=client_id,
                request_id=request_id,
                round_no=round_no,
                stage=stage,
                llm_provider=runtime['provider'],
                model_name=runtime['model'],
                request_payload=request_payload,
                response_payload=response_payload,
            )
            logger.info(
                'llm response stage=%s requested=%s actual=%s model=%s chars=%s conversation_id=%s request_id=%s',
                stage,
                requested_provider,
                runtime['provider'],
                runtime['model'],
                len(content),
                conversation_id or '',
                request_id or '',
            )
            return {
                'provider': requested_provider,
                'label': requested_meta['label'],
                'model': runtime['model'],
                'max_input_tokens': requested_meta['max_input_tokens'],
                'content': content,
                'actual_provider': runtime['provider'],
                'actual_label': runtime['label'],
                'execution_plan': execution_plan,
                'security_level': security_level,
            }
        except Exception as exc:  # noqa: BLE001
            errors.append(f'{actual_provider}: {exc}')
            insert_llm_invocation_log(
                conversation_id=conversation_id,
                client_id=client_id,
                request_id=request_id,
                round_no=round_no,
                stage=stage,
                llm_provider=actual_provider,
                model_name=(LLM_PROVIDER_CONFIGS.get(actual_provider) or {}).get('model', ''),
                request_payload=request_payload,
                error_message=str(exc),
            )
            logger.exception(
                'llm request failed stage=%s requested=%s actual=%s conversation_id=%s request_id=%s error=%s',
                stage,
                requested_provider,
                actual_provider,
                conversation_id or '',
                request_id or '',
                exc,
            )
    raise ValueError(f'模型调用全部失败：{" | ".join(errors)}')
