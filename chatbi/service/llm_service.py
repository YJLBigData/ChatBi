from typing import Any

from openai import OpenAI

from chatbi.config import (
    DEFAULT_LLM_PROVIDER,
    LLM_PROVIDER_ALIASES,
    LLM_PROVIDER_CONFIGS,
    LLM_REQUEST_TIMEOUT_SECONDS,
)
from chatbi.repository.task_repository import insert_llm_invocation_log


def normalize_llm_provider(raw_value: Any) -> str:
    value = str(raw_value or '').strip().lower()
    return LLM_PROVIDER_ALIASES.get(value, '')


def resolve_default_llm_provider() -> str:
    configured = normalize_llm_provider(DEFAULT_LLM_PROVIDER) or 'bailian'
    if LLM_PROVIDER_CONFIGS.get(configured, {}).get('api_key'):
        return configured
    for provider_name in ['bailian', 'deepseek']:
        if LLM_PROVIDER_CONFIGS.get(provider_name, {}).get('api_key'):
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
    }


def get_llm_runtime(provider_name: str | None = None) -> dict[str, Any]:
    meta = get_llm_provider_meta(provider_name)
    resolved_provider = meta['provider']
    config = LLM_PROVIDER_CONFIGS.get(resolved_provider)
    if not config:
        raise ValueError('不支持的模型引擎')
    if not config.get('api_key'):
        env_name = 'DASHSCOPE_API_KEY' if resolved_provider == 'bailian' else 'DEEPSEEK_API_KEY'
        raise ValueError(f'缺少 {env_name}，请先在 .env 中配置')
    return {
        **meta,
        'client': OpenAI(
            api_key=config['api_key'],
            base_url=config['base_url'],
            timeout=LLM_REQUEST_TIMEOUT_SECONDS,
            max_retries=1,
        ),
    }


def chat_completion(
    *,
    stage: str,
    messages: list[dict[str, str]],
    provider_name: str | None,
    conversation_id: str | None = None,
    client_id: str | None = None,
    temperature: float = 0,
) -> dict[str, Any]:
    runtime = get_llm_runtime(provider_name)
    request_payload = {
        'model': runtime['model'],
        'messages': messages,
        'temperature': temperature,
    }
    try:
        completion = runtime['client'].chat.completions.create(
            model=runtime['model'],
            messages=messages,
            temperature=temperature,
        )
        content = completion.choices[0].message.content or ''
        response_payload = {'content': content}
        insert_llm_invocation_log(
            conversation_id=conversation_id,
            client_id=client_id,
            stage=stage,
            llm_provider=runtime['provider'],
            model_name=runtime['model'],
            request_payload=request_payload,
            response_payload=response_payload,
        )
        return {
            'provider': runtime['provider'],
            'label': runtime['label'],
            'model': runtime['model'],
            'max_input_tokens': runtime['max_input_tokens'],
            'content': content,
        }
    except Exception as exc:  # noqa: BLE001
        insert_llm_invocation_log(
            conversation_id=conversation_id,
            client_id=client_id,
            stage=stage,
            llm_provider=runtime['provider'],
            model_name=runtime['model'],
            request_payload=request_payload,
            error_message=str(exc),
        )
        raise
