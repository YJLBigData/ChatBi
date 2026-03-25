from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any


class OllamaClient:
    def __init__(self, *, base_url: str, model: str, timeout: int = 90):
        self.base_url = base_url.rstrip('/')
        self.model = model
        self.timeout = timeout

    def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        request = urllib.request.Request(
            f'{self.base_url}{path}',
            data=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
            headers={'Content-Type': 'application/json'},
            method='POST',
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read().decode('utf-8')
        except urllib.error.HTTPError as exc:  # noqa: PERF203
            detail = exc.read().decode('utf-8', errors='ignore')
            raise ValueError(f'Ollama 请求失败: HTTP {exc.code} {detail}') from exc
        except urllib.error.URLError as exc:
            raise ValueError(f'Ollama 连接失败: {exc.reason}') from exc
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise ValueError(f'Ollama 返回非法 JSON: {body[:300]}') from exc
        if not isinstance(payload, dict):
            raise ValueError('Ollama 返回结构非法')
        return payload

    def chat(self, messages: list[dict[str, str]], *, temperature: float = 0) -> str:
        payload = {
            'model': self.model,
            'messages': messages,
            'stream': False,
            'keep_alive': '30m',
            'options': {
                'temperature': temperature,
            },
        }
        response = self._post_json('/api/chat', payload)
        message = response.get('message') or {}
        content = str(message.get('content') or '').strip()
        if not content:
            raise ValueError('Ollama 未返回有效内容')
        return content

    def rewrite(self, prompt: str) -> str:
        return self.chat([
            {'role': 'system', 'content': '你是问题改写助手。请保留原意，输出更清晰、精炼的单句问题。'},
            {'role': 'user', 'content': prompt},
        ])

    def classify(self, prompt: str) -> str:
        return self.chat([
            {'role': 'system', 'content': '你是分类助手。只输出最合适的分类结果。'},
            {'role': 'user', 'content': prompt},
        ])

    def clarify(self, prompt: str) -> str:
        return self.chat([
            {'role': 'system', 'content': '你是澄清助手。请输出一句简洁明确的澄清问题。'},
            {'role': 'user', 'content': prompt},
        ])
