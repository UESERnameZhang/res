"""Core framework: message bus, agent base, registry, event types."""
from __future__ import annotations

import asyncio
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional


# ─── Enums ───
class AgentCapability(str, Enum):
    ORCHESTRATION = "orchestration"
    MONITORING = "monitoring"
    DEPLOYMENT = "deployment"
    LOG_ANALYSIS = "log_analysis"
    REMEDIATION = "remediation"
    REPORTING = "reporting"


class AgentStatus(str, Enum):
    IDLE = "idle"
    BUSY = "busy"
    ERROR = "error"
    OFFLINE = "offline"


# ─── Data structures ───
@dataclass
class TaskContext:
    task_id: str
    workflow_id: str
    capability: str = "orchestration"
    priority: int = 50
    params: Dict[str, Any] = field(default_factory=dict)
    result: Dict[str, Any] = field(default_factory=dict)
    status: str = "pending"
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3


# ─── Message Bus (in-memory) ───
class MessageBus:
    """Simple in-memory message bus using asyncio.Queue per stream."""

    def __init__(self):
        self._streams: Dict[str, asyncio.Queue] = {}
        self._lock = asyncio.Lock()

    async def get_stream(self, name: str) -> asyncio.Queue:
        if name not in self._streams:
            async with self._lock:
                if name not in self._streams:
                    self._streams[name] = asyncio.Queue()
        return self._streams[name]

    async def publish(self, stream: str, ctx: TaskContext) -> None:
        q = await self.get_stream(stream)
        await q.put(ctx)

    async def consume(self, stream: str, timeout: float = 5.0) -> Optional[TaskContext]:
        q = await self.get_stream(stream)
        try:
            return await asyncio.wait_for(q.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None


# ─── Agent Base ───
class BaseAgent(ABC):
    CAPABILITY: AgentCapability
    INPUT_STREAM: str

    def __init__(self, agent_id: Optional[str] = None, bus: Optional[MessageBus] = None):
        self.agent_id = agent_id or f"{self.INPUT_STREAM}-{uuid.uuid4().hex[:6]}"
        self.bus = bus
        self.status = AgentStatus.IDLE
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    @abstractmethod
    async def handle(self, ctx: TaskContext) -> Optional[TaskContext]:
        """Process one task. Return result context or None."""
        ...

    def route_to(self, ctx: TaskContext) -> List[str]:
        """Return list of stream names to forward results to."""
        return []

    async def run(self) -> None:
        self._stop_event.clear()
        self._task = asyncio.create_task(self._event_loop())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            self._task.cancel()

    async def _event_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                ctx = await self.bus.consume(self.INPUT_STREAM)
                if ctx is None:
                    continue
                self.status = AgentStatus.BUSY
                result = await self.handle(ctx)
                self.status = AgentStatus.IDLE

                if result:
                    for stream in self.route_to(result):
                        await self.bus.publish(stream, result)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.status = AgentStatus.ERROR
                print(f"[{self.agent_id}] Error: {e}")


# ─── Registry ───
class AgentRegistry:
    def __init__(self):
        self._agents: Dict[str, BaseAgent] = {}

    def register(self, agent: BaseAgent) -> None:
        self._agents[agent.agent_id] = agent

    def get_by_capability(self, cap: AgentCapability) -> List[BaseAgent]:
        return [a for a in self._agents.values() if a.CAPABILITY == cap]

    def list_all(self) -> List[BaseAgent]:
        return list(self._agents.values())

    def get_stats(self) -> dict:
        caps = {}
        for a in self._agents.values():
            c = a.CAPABILITY.value
            caps[c] = caps.get(c, 0) + 1
        return {"total": len(self._agents), "by_capability": caps}
