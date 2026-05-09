"""Pluggable AI backends: mock (default), claude, gpt, ollama."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import json


@dataclass
class LLMResponse:
    content: str
    model: str = "unknown"
    usage: Dict[str, int] = field(default_factory=dict)


class BaseLLM(ABC):
    @abstractmethod
    async def chat(self, prompt: str, system: str = "") -> LLMResponse:
        ...


# ─── Mock (default, no API needed) ───
class MockLLM(BaseLLM):
    async def chat(self, prompt: str, system: str = "") -> LLMResponse:
        # Return structured JSON for task decomposition scenarios
        if "decompose" in prompt.lower() or "分解" in prompt:
            return LLMResponse(
                content=json.dumps({
                    "analysis": "Mock analysis result",
                    "subtasks": [
                        {"capability": "log_analysis", "description": "Analyze recent error logs"},
                        {"capability": "monitoring", "description": "Check dependent services"},
                        {"capability": "deployment", "description": "Check recent deployments"},
                    ],
                    "severity": "warning",
                    "recommended_action": "investigate",
                }, ensure_ascii=False),
                model="mock",
            )
        if "report" in prompt.lower() or "报告" in prompt:
            return LLMResponse(
                content="## Incident Report\n\n**Root Cause:** Mock analysis\n**Impact:** Low\n**Action:** No action needed.",
                model="mock",
            )
        if "log" in prompt.lower() or "日志" in prompt:
            return LLMResponse(
                content=json.dumps({
                    "anomalies": ["Memory usage spike at T+2m"],
                    "error_patterns": ["OutOfMemoryError x12"],
                    "suspected_root_cause": "Memory leak in recent deployment",
                }, ensure_ascii=False),
                model="mock",
            )
        return LLMResponse(content="Mock response for: " + prompt[:100], model="mock")


# ─── Claude API ───
class ClaudeLLM(BaseLLM):
    def __init__(self, api_key: str, model: str = "claude-sonnet-4-6-20250514"):
        self.api_key = api_key
        self.model = model

    async def chat(self, prompt: str, system: str = "") -> LLMResponse:
        try:
            from anthropic import AsyncAnthropic
            client = AsyncAnthropic(api_key=self.api_key)
            msg = await client.messages.create(
                model=self.model,
                max_tokens=4096,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            )
            return LLMResponse(
                content=msg.content[0].text,
                model=self.model,
                usage={"input": msg.usage.input_tokens, "output": msg.usage.output_tokens},
            )
        except ImportError:
            return LLMResponse(content="[Claude SDK not installed] " + prompt[:100], model="claude-fallback")


# ─── GPT API ───
class GPTLLM(BaseLLM):
    def __init__(self, api_key: str, model: str = "gpt-4o", base_url: str = ""):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url

    async def chat(self, prompt: str, system: str = "") -> LLMResponse:
        try:
            from openai import AsyncOpenAI
            client = AsyncOpenAI(api_key=self.api_key, base_url=self.base_url or None)
            resp = await client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt},
                ],
            )
            return LLMResponse(
                content=resp.choices[0].message.content or "",
                model=self.model,
                usage={"input": resp.usage.prompt_tokens, "output": resp.usage.completion_tokens},
            )
        except ImportError:
            return LLMResponse(content="[OpenAI SDK not installed] " + prompt[:100], model="gpt-fallback")


# ─── Ollama (local) ───
class OllamaLLM(BaseLLM):
    def __init__(self, model: str = "llama3", base_url: str = "http://localhost:11434"):
        self.model = model
        self.base_url = base_url

    async def chat(self, prompt: str, system: str = "") -> LLMResponse:
        try:
            import httpx
            async with httpx.AsyncClient(base_url=self.base_url, timeout=120) as cli:
                resp = await cli.post("/api/chat", json={
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": prompt},
                    ],
                    "stream": False,
                })
                data = resp.json()
                return LLMResponse(content=data.get("message", {}).get("content", ""), model=self.model)
        except ImportError:
            return LLMResponse(content="[httpx not installed] " + prompt[:100], model="ollama-fallback")


# ─── Factory ───
def create_llm(provider: str = "mock", **kwargs) -> BaseLLM:
    providers = {
        "mock": lambda: MockLLM(),
        "claude": lambda: ClaudeLLM(api_key=kwargs.get("api_key", ""), model=kwargs.get("model", "claude-sonnet-4-6-20250514")),
        "gpt": lambda: GPTLLM(api_key=kwargs.get("api_key", ""), model=kwargs.get("model", "gpt-4o"), base_url=kwargs.get("base_url", "")),
        "ollama": lambda: OllamaLLM(model=kwargs.get("model", "llama3"), base_url=kwargs.get("base_url", "http://localhost:11434")),
    }
    factory = providers.get(provider, providers["mock"])
    return factory()
