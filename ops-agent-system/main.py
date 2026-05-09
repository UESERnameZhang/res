#!/usr/bin/env python3
"""Multi-Agent Collaborative Ops Automation System - Demo Runner."""
from __future__ import annotations

import asyncio
import uuid
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.core import AgentCapability, MessageBus, AgentRegistry, TaskContext
from src.agents import (
    OrchestratorAgent,
    MonitorAgent,
    DeployAgent,
    LogAnalyzerAgent,
    RemediationAgent,
    ReportAgent,
)
from src.config import AI_PROVIDER, AI_MODEL, AI_API_KEY, AI_BASE_URL
from src.ai_backends import create_llm


async def demo_alert_to_recovery():
    """Demo: Monitoring alert -> orchestration -> analysis -> remediation -> report."""
    print("\n" + "=" * 60)
    print("  Multi-Agent Ops Automation System - Demo")
    print("=" * 60)

    # 1. Init
    bus = MessageBus()
    registry = AgentRegistry()
    llm = create_llm(AI_PROVIDER, api_key=AI_API_KEY, model=AI_MODEL, base_url=AI_BASE_URL)
    print(f"\n[System] AI Backend: {AI_PROVIDER} / {AI_MODEL}")

    # 2. Create agents
    orchestrator = OrchestratorAgent(bus=bus, llm=llm)
    monitor = MonitorAgent(bus=bus, llm=llm)
    deploy = DeployAgent(bus=bus)
    log_analyzer = LogAnalyzerAgent(bus=bus, llm=llm)
    remediation = RemediationAgent(bus=bus)
    report = ReportAgent(bus=bus, llm=llm)

    for a in [orchestrator, monitor, deploy, log_analyzer, remediation, report]:
        registry.register(a)

    print(f"\n[System] Registered {registry.get_stats()['total']} agents:")
    for cap, count in registry.get_stats()["by_capability"].items():
        print(f"  - {cap}: {count} instance(s)")

    # 3. Start all agents
    await asyncio.gather(*[a.run() for a in registry.list_all()])
    print("\n[System] All agents started.\n")

    await asyncio.sleep(0.2)

    # 4. Scenario: Production CPU spike triggers alert
    print("-" * 60)
    print("SCENARIO: Production api-gateway CPU 92% → Multi-Agent Response")
    print("-" * 60)

    alert_ctx = TaskContext(
        task_id=f"alert-{uuid.uuid4().hex[:8]}",
        workflow_id=f"wf-{uuid.uuid4().hex[:6]}",
        capability="monitoring",
        priority=90,
        params={
            "alert_type": "cpu_spike",
            "service_name": "api-gateway",
            "metric": "cpu_usage",
            "value": "92%",
            "threshold": "80%",
            "duration": "3m",
            "escalate_on_critical": True,
        },
    )

    print(f"\n> Triggering alert: {alert_ctx.task_id}")
    await bus.publish("stream:monitoring", alert_ctx)

    # 5. Wait for workflow to complete
    print("\n[Agent Collaboration Trace]")
    print("-" * 40)

    # Give agents time to process the workflow chain:
    # Monitor → Orchestrator → (LogAnalyzer + Deploy + Monitor) → Orchestrator → Remediation → Report
    await asyncio.sleep(5.0)

    print("\n" + "=" * 60)
    print("  Demo Complete - System Architecture Summary")
    print("=" * 60)
    print("""
  ┌──────────┐     ┌───────────────┐     ┌──────────┐
  │ Monitor  │────→│ Orchestrator  │────→│  Report  │
  │ Agent    │     │ Agent         │     │  Agent   │
  └──────────┘     └───┬───┬───┬───┘     └──────────┘
                       │   │   │
              ┌────────┘   │   └────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │LogAnalyze│ │ Monitor  │ │ Deploy   │
        │Agent     │ │ Agent    │ │ Agent    │
        └──────────┘ └──────────┘ └────┬─────┘
                                       │
                                ┌──────▼──────┐
                                │ Remediation │
                                │ Agent       │
                                └─────────────┘

  Streams: stream:orchestrator | stream:monitoring | stream:deployment
           stream:log_analysis | stream:remediation | stream:reporting
""")

    # 6. Stop all agents
    for a in registry.list_all():
        await a.stop()
    print("[System] All agents stopped.")


async def custom_task(task_type: str, **params):
    """Submit a custom task to the system."""
    bus = MessageBus()
    registry = AgentRegistry()
    llm = create_llm(AI_PROVIDER, api_key=AI_API_KEY, model=AI_MODEL, base_url=AI_BASE_URL)

    for cls in [OrchestratorAgent, MonitorAgent, DeployAgent, LogAnalyzerAgent, RemediationAgent, ReportAgent]:
        agent = cls(bus=bus, llm=llm) if cls in (OrchestratorAgent, MonitorAgent, LogAnalyzerAgent, ReportAgent) else cls(bus=bus)
        registry.register(agent)

    await asyncio.gather(*[a.run() for a in registry.list_all()])

    capability_map = {
        "deploy": "deployment",
        "analyze": "log_analysis",
        "remediate": "remediation",
        "monitor": "monitoring",
        "report": "reporting",
    }

    ctx = TaskContext(
        task_id=f"task-{uuid.uuid4().hex[:8]}",
        workflow_id=f"wf-{uuid.uuid4().hex[:6]}",
        capability=capability_map.get(task_type, task_type),
        params=params,
    )
    await bus.publish(f"stream:{capability_map.get(task_type, task_type)}", ctx)
    await asyncio.sleep(3.0)

    for a in registry.list_all():
        await a.stop()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        task_type = sys.argv[1]
        params = dict(arg.split("=") for arg in sys.argv[2:] if "=" in arg)
        asyncio.run(custom_task(task_type, **params))
    else:
        asyncio.run(demo_alert_to_recovery())
